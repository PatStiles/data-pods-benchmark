use plex::*;

use cowlang::ast::Span;
use cowlang::{get_next_indent, parse_indents, IndentResult};

use serde::{Deserialize, Serialize};

use crate::application_parser::Argument;

use std::collections::BTreeMap;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum MetaOperation {
    AssertEquals {
        lhs: String,
        rhs: String,
    },
    AssertNotEquals {
        lhs: String,
        rhs: String,
    },
    Call {
        fn_name: String,
        args: Vec<String>,
        alias: Option<String>,
    },
}

pub type MetaStage = Vec<MetaOperation>;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MetaFnDefinition {
    pub args: Vec<Argument>,
    stages: Vec<MetaStage>,
}

impl MetaFnDefinition {
    pub fn new(args: Vec<Argument>, stages: Vec<MetaStage>) -> Self {
        Self { args, stages }
    }

    pub fn num_stages(&self) -> u32 {
        self.stages.len() as u32
    }

    pub fn get_stage(&self, stage_id: u32) -> &MetaStage {
        &self.stages[stage_id as usize]
    }
}

struct Lexer<'a> {
    original: &'a str,
    remaining: &'a str,
    at_start: bool,
    at_end: bool,
    empty_line: bool,

    indents: BTreeMap<usize, i32>,
    position: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(s: &'a str) -> Lexer<'a> {
        let indents = parse_indents(s);

        let position = 0;
        let at_start = true;
        let at_end = false;
        let empty_line = true;

        Self {
            original: s,
            remaining: s,
            indents,
            position,
            at_start,
            at_end,
            empty_line,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Token {
    Whitespace,
    Colon,
    As,
    Call,
    Comma,
    Assert,
    Indent,
    Dedent,
    Newline,
    Equals,
    Stage,
    NotEquals,
    OpenBracket,
    CloseBracket,
    Identifier(String),
    Comment(String),
}

use Token::*;

lexer! {
    fn take_token(tok: 'a) -> Token;
    "assert" => Token::Assert,
    "call" => Token::Call,
    "stage" => Token::Stage,
    ":" => Token::Colon,
    "," => Token::Comma,
    "==" => Token::Equals,
    "!=" => Token::NotEquals,
    r"\(" => Token::OpenBracket,
    r"\)" => Token::CloseBracket,
    "as" => Token::As,
    r"[ \t\r]" => Token::Whitespace,
    r"\n" => Token::Newline,
    r#"[a-zA-Z_][a-zA-Z0-9_]*"# => Token::Identifier(tok.into()),
    r"\#[^\n]*" => Token::Comment(tok.into())
}

impl<'a> Iterator for Lexer<'a> {
    type Item = (Token, Span);
    fn next(&mut self) -> Option<(Token, Span)> {
        // skip over whitespace and comments
        loop {
            if let Some(res) = get_next_indent(self.position, &mut self.indents) {
                let token = match res {
                    IndentResult::Newline => Token::Newline,
                    IndentResult::Indent => Token::Indent,
                    IndentResult::Dedent => Token::Dedent,
                };

                let ipos = self.position;
                let span = Span { lo: ipos, hi: ipos };

                self.empty_line = false;
                return Some((token, span));
            }

            let (tok, span) = if let Some((tok, new_remaining)) = take_token(self.remaining) {
                let lo = self.original.len() - self.remaining.len();
                let hi = self.original.len() - new_remaining.len();
                self.remaining = new_remaining;
                (tok, Span { lo, hi })
            } else {
                // Return EOF token exactly once
                if self.at_end {
                    return None;
                } else {
                    self.at_end = true;

                    if self.at_start {
                        // parser gets confused on empty file
                        // so do not insert a newline here
                        //
                        // TODO should an empty file be a vaild source?
                        return None;
                    } else {
                        // Treat EOF as new line
                        (
                            Token::Newline,
                            Span {
                                lo: self.original.len(),
                                hi: self.original.len(),
                            },
                        )
                    }
                }
            };

            self.position = span.hi;

            match tok {
                Token::Whitespace | Token::Comment { 0: _ } => {
                    continue;
                }
                // ignore empty lines
                Token::Newline => {
                    if self.empty_line {
                        continue;
                    } else {
                        self.empty_line = true;
                        return Some((tok, span));
                    }
                }
                tok => {
                    self.at_start = false;
                    self.empty_line = false;
                    return Some((tok, span));
                }
            }
        }
    }
}

#[derive(Debug)]
enum Expr {
    Stage {
        stmts: Vec<ParseNode>,
    },
    Call {
        fn_name: String,
        args: Vec<ParseNode>,
        alias: Option<String>,
    },
    Argument {
        name: String,
    },
    AssertEquals {
        lhs: String,
        rhs: String,
    },
    AssertNotEquals {
        lhs: String,
        rhs: String,
    },
}

type ParseNode = (Span, Expr);

parser! {
    fn parse_(Token, Span);

    // combine two spans
    (a, b) {
        Span {
            lo: a.lo,
            hi: b.hi,
        }
    }

    stages: Vec<ParseNode> {
        stages[mut st] stage[rhs] => {
            st.push(rhs);
            st
        }
        stages[st] Newline => {
            st
        }
        => vec![]
    }

    stage: ParseNode {
        Stage Colon Newline Indent statements[stmts] Dedent => {
            (span!(), Expr::Stage{stmts})
        }
    }

    statements: Vec<ParseNode> {
        statements[mut st] definition[rhs] => {
            st.push(rhs);
            st
        }
        statements[st] Newline => {
            st
        }
        => vec![]
    }

    definition: ParseNode {
        Call Identifier(fn_name) OpenBracket arguments[args] CloseBracket =>
            ( span!(), Expr::Call{fn_name, args, alias: None} ),
        Call Identifier(fn_name) OpenBracket arguments[args] CloseBracket As Identifier(alias) =>
            ( span!(), Expr::Call{fn_name, args, alias: Some(alias)} ),
        Assert assert_stmt[stmt] => stmt
    }

    assert_stmt: ParseNode {
        Identifier(lhs) Equals Identifier(rhs) =>
            ( span!(), Expr::AssertEquals{lhs, rhs}),
        Identifier(lhs) NotEquals Identifier(rhs) =>
            ( span!(), Expr::AssertNotEquals{lhs, rhs})
    }

    arguments: Vec<ParseNode> {
        arguments[mut args] Comma argument[arg] => {
            args.push(arg);
            args
        }
        argument[arg] => {
            vec!(arg)
        }
        => vec![]
    }

    argument: ParseNode {
        Identifier(name) => {
            (span!(), Expr::Argument{name})
        }
    }
}

pub fn compile_string(args: Vec<Argument>, input: &str) -> MetaFnDefinition {
    #[cfg(feature = "verbose-compile")]
    let lexer = Lexer::new(input).inspect(|elem| println!("{:?}", elem));

    #[cfg(not(feature = "verbose-compile"))]
    let lexer = Lexer::new(input);

    let mut ast = match parse_(lexer) {
        Ok(p) => p,
        Err((info, e)) => {
            panic!("{}", cowlang::generate_compile_error(input, info, e));
        }
    };

    let mut stages = Vec::new();

    for (_span, elem) in ast.drain(..) {
        if let Expr::Stage { mut stmts } = elem {
            let mut stage = Vec::new();

            for (_span, elem) in stmts.drain(..) {
                let op = match elem {
                    Expr::AssertEquals { lhs, rhs } => MetaOperation::AssertEquals { lhs, rhs },
                    Expr::AssertNotEquals { lhs, rhs } => {
                        MetaOperation::AssertNotEquals { lhs, rhs }
                    }
                    Expr::Call {
                        fn_name,
                        args,
                        alias,
                    } => {
                        let mut arg_strs = Vec::new();

                        for (_span, elem) in args {
                            if let Expr::Argument { name } = elem {
                                arg_strs.push(name);
                            } else {
                                panic!("invalid state");
                            }
                        }

                        MetaOperation::Call {
                            fn_name,
                            args: arg_strs,
                            alias,
                        }
                    }
                    _ => panic!("Got unexpected parse node: {:?}", elem),
                };

                stage.push(op);
            }

            if stage.is_empty() {
                panic!("Empty stage");
            }

            stages.push(stage);
        } else {
            panic!("Unexpected parse node");
        }
    }

    MetaFnDefinition { args, stages }
}
