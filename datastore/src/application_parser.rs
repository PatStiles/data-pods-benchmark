use plex::*;

use serde::{Deserialize, Serialize};

use cowlang::ast::{Program, Span};
use cowlang::{PrimitiveType, TypeDefinition};

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::objects::ObjectFieldMap;

use crate::meta_fn_parser::MetaFnDefinition;

use crate::meta_fn_parser as meta_fn;

#[derive(Debug, PartialEq)]
enum Token {
    Whitespace,
    Newline,
    Function,
    ObjectType,
    Public,
    Global,
    GlobalFunction,
    MetaFunction,
    OpenSquareBracket,
    CloseSquareBracket,
    OpenBracket,
    CloseBracket,
    TemplateOpen,
    TemplateClose,
    Comma,
    TypeAny,
    TypeBool,
    TypeString,
    TypeI64,
    TypeU64,
    TypeU8,
    TypeF64,
    TypeMap,
    TypeList,
    TypeBytes,
    Semicolon,
    Colon,
    I64Literal(i64),
    CodeLine(String),
    Identifier(String),
    Comment(String),
}

use Token::*;

enum Expr {
    ObjectType {
        name: String,
        fields: Vec<Argument>,
    },
    MetaFunction {
        name: String,
        args: Vec<Argument>,
        content: String,
    },
    GlobalFunction {
        name: String,
        args: Vec<Argument>,
        content: String,
        public: bool,
    },
    Function {
        name: String,
        args: Vec<Argument>,
        content: String,
        public: bool,
    },
    Global {
        name: String,
        value_type: TypeDefinition,
    },
}

type ParseNode = (Span, Expr);

pub type Argument = (String, TypeDefinition);
pub type FunctionDefinition = (Vec<Argument>, Arc<Program>, bool);
pub type GlobalFnDefinition = (Vec<Argument>, Arc<Program>, bool);

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct AppDefinition {
    pub types: BTreeMap<String, (u16, ObjectFieldMap)>,
    pub functions: HashMap<String, FunctionDefinition>,
    //FIXME using arc here is kind of ugly because it requires the
    // 'rc' feature in serde. However, it avoid some copying of data.
    // Let's find a better way to do this!
    pub meta_fns: HashMap<String, Arc<MetaFnDefinition>>,
    pub global_fns: HashMap<String, FunctionDefinition>,
    pub globals: HashMap<String, TypeDefinition>,
}

lexer! {
    fn take_token(tok: 'a) -> Token;
    r"[ \t\r]" => Token::Whitespace,
    r"\n" => Token::Newline,
    "pub" => Token::Public,
    "Any" => Token::TypeAny,
    "Bool" => Token::TypeBool,
    "String" => Token::TypeString,
    "Map" => Token::TypeMap,
    "Bytes" => Token::TypeBytes,
    "List" => Token::TypeList,
    "i64" => Token::TypeI64,
    "u64" => Token::TypeU64,
    "u8" => Token::TypeU8,
    "f64" => Token::TypeF64,
    "Map" => Token::TypeMap,
    "List" => Token::TypeList,
    "Bytes" => Token::TypeBytes,
    "ObjectId" => Token::TypeBytes, //TODO no real objectid type yet, just an alias for bytes
    r":" => Token::Colon,
    r";" => Token::Semicolon,
    "<" => Token::TemplateOpen,
    ">" => Token::TemplateClose,
    "global" => Token::Global,
    "meta_fn" => Token::MetaFunction,
    "global_fn" => Token::GlobalFunction,
    "fn" => Token::Function,
    "type" => Token::ObjectType,
    r"\[" => Token::OpenSquareBracket,
    r"\]" => Token::CloseSquareBracket,
    r"\(" => Token::OpenBracket,
    r"\)" => Token::CloseBracket,
    r"," => Token::Comma,
    "[0-9]+" => Token::I64Literal(tok.parse().unwrap()),
    r#"[a-zA-Z_][a-zA-Z0-9_]*"# => Token::Identifier(tok.into()),
    r"\#[^\n]*" => Token::Comment(tok.into())
}

struct Lexer<'a> {
    original: &'a str,
    remaining: &'a str,
    is_newline: bool,

    // allow for variable size indent
    indent_depth: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(s: &'a str) -> Lexer<'a> {
        Self {
            original: s,
            remaining: s,
            is_newline: true,
            indent_depth: 0,
        }
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = (Token, Span);
    fn next(&mut self) -> Option<(Token, Span)> {
        loop {
            if self.is_newline {
                self.is_newline = false;
                let mut start = 0;

                for (pos, c) in self.remaining.chars().enumerate() {
                    if self.indent_depth > 0 && pos == self.indent_depth {
                        start = pos;
                        break;
                    }

                    if c != ' ' {
                        if c == '\n' && self.indent_depth > 0 {
                            //empty line. don't modify indent
                            start = pos;

                            let tok = Token::CodeLine("".to_string());
                            let lo = start;
                            let hi = start;

                            return Some((tok, Span { lo, hi }));
                        } else if pos > 0 {
                            start = pos;
                            self.indent_depth = start;
                        } else {
                            // reset indent
                            self.indent_depth = 0;
                        }
                        break;
                    }
                }

                if start > 0 {
                    let mut end = 0;

                    for (pos, c) in self.remaining.chars().enumerate() {
                        if c == '\n' {
                            end = pos;
                            break;
                        }
                    }

                    let tok = if end > start {
                        let s = self.remaining[start..end].to_string();

                        Token::CodeLine(s)
                    } else {
                        Token::CodeLine("".to_string())
                    };

                    let new_remaining = &self.remaining[end..];

                    let lo = self.original.len() - self.remaining.len();
                    let hi = self.original.len() - new_remaining.len();

                    self.remaining = new_remaining;
                    return Some((tok, Span { lo, hi }));
                } else {
                    self.indent_depth = 0;
                }
            }

            let (tok, span) = if let Some((tok, new_remaining)) = take_token(self.remaining) {
                let lo = self.original.len() - self.remaining.len();
                let hi = self.original.len() - new_remaining.len();
                self.remaining = new_remaining;

                (tok, Span { lo, hi })
            } else {
                return None;
            };

            match tok {
                Token::Whitespace | Token::Comment { 0: _ } => {
                    continue;
                }
                tok => {
                    if tok == Token::Newline {
                        self.is_newline = true;
                    } else {
                        self.is_newline = false;
                    }

                    return Some((tok, span));
                }
            }
        }
    }
}

parser! {
    fn parse_(Token, Span);

    // combine two spans
    (a, b) {
        Span {
            lo: a.lo,
            hi: b.hi,
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
        ObjectType Identifier(id) OpenBracket args[fields] CloseBracket =>
            ( span!(), Expr::ObjectType{name: id, fields} ),
        Function Identifier(id) OpenBracket args[a] CloseBracket Colon Newline code[code] Newline =>
            ( span!(), Expr::Function{name: id, args: a, content: code, public: false} ),
        Public Function Identifier(id) OpenBracket args[a] CloseBracket Colon Newline code[code] Newline =>
            ( span!(), Expr::Function{name: id, args: a, content: code, public: true} ),
        GlobalFunction Identifier(id) OpenBracket args[a] CloseBracket Colon Newline code[code] Newline =>
            ( span!(), Expr::GlobalFunction{name: id, args: a, content: code, public: false} ),
        Public GlobalFunction Identifier(id) OpenBracket args[a] CloseBracket Colon Newline code[code] Newline =>
            ( span!(), Expr::GlobalFunction{name: id, args: a, content: code, public: true} ),
        Public MetaFunction Identifier(id) OpenBracket args[a] CloseBracket Colon Newline code[code] Newline =>
            ( span!(), Expr::MetaFunction{name: id, args: a, content: code} ),
        Global Identifier(id) Colon value_type[t] =>
            ( span!(), Expr::Global{name: id, value_type: t })
    }

    code: String {
        code[pre] Newline CodeLine(s) => {
            pre + "\n" + &s
        }
        CodeLine(s) => { s }
    }

    args: Vec<Argument> {
        args[mut args] Comma argument[arg] => {
            args.push(arg);
            args
        }
        argument[arg] => {
            vec!(arg)
        }
        => vec![]
    }

    argument: Argument {
        Identifier(id) Colon value_type[t] => {
            (id, t)
        }
    }

    value_type: TypeDefinition {
        TypeAny => TypeDefinition::Primitive(PrimitiveType::Any),
        TypeBool=> TypeDefinition::Primitive(PrimitiveType::Bool),
        TypeString => TypeDefinition::Primitive(PrimitiveType::String),
        TypeBytes => TypeDefinition::Bytes,
        TypeU64 => TypeDefinition::Primitive(PrimitiveType::U64),
        TypeI64 => TypeDefinition::Primitive(PrimitiveType::I64),
        TypeU8 => TypeDefinition::Primitive(PrimitiveType::U8),
        TypeF64 => TypeDefinition::Primitive(PrimitiveType::F64),
        TypeMap TemplateOpen value_type[lhs] Comma value_type[rhs] TemplateClose => {
            TypeDefinition::Map(Box::new(lhs), Box::new(rhs))
        }

        TypeList TemplateOpen value_type[vtype] TemplateClose => {
            TypeDefinition::List(Box::new(vtype))
        }
        TypeBytes => { TypeDefinition::Bytes }

        OpenSquareBracket value_type[t] Semicolon I64Literal(len) CloseSquareBracket => {
            if len <= 0 {
                panic!("Array length must be >0");
            }

            TypeDefinition::Array(Box::new(t), len as usize)
        }
    }

    linebreak: () {
        linebreak Newline => {}
        Newline => {}
    }
}

pub fn compile_app_definition(input: &str) -> AppDefinition {
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

    let mut result = AppDefinition::default();
    let mut next_type_id = 0;

    for (_span, elem) in ast.drain(..) {
        match elem {
            Expr::Function {
                name,
                args,
                content,
                public,
            } => {
                let program = cowlang::compile_string(&content);
                result
                    .functions
                    .insert(name, (args, Arc::new(program), public));
            }
            Expr::GlobalFunction {
                name,
                args,
                content,
                public,
            } => {
                let program = cowlang::compile_string(&content);
                result
                    .global_fns
                    .insert(name, (args, Arc::new(program), public));
            }
            Expr::MetaFunction {
                name,
                args,
                content,
            } => {
                let definition = meta_fn::compile_string(args, &content);
                result.meta_fns.insert(name, Arc::new(definition));
            }
            Expr::Global { name, value_type } => {
                result.globals.insert(name, value_type);
            }
            Expr::ObjectType { name, mut fields } => {
                let mut field_map = ObjectFieldMap::default();

                for (pos, (name, ftype)) in fields.drain(..).enumerate() {
                    field_map.insert(name, (pos, ftype));
                }

                result.types.insert(name, (next_type_id, field_map));
                next_type_id += 1;
            }
        }
    }

    result
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn define_empty() {
        let def = compile_app_definition(
            "\
            fn my_func(arg_one: String, arg_two: i64):\
          \n       #some comment\
          \n       #another line\n\
          \n\
          \nfn my_other_func(arg_one: String, arg_two: i64):\
          \n       #some comment\n\
          \n",
        ); //FIXME get rid of empty line at the end

        assert_eq!(def.functions.len(), 2);

        let (args, _, _) = &def.functions["my_func"];
        let (name, atype) = &args[1];

        assert_eq!("arg_two", name);
        assert_eq!(*atype, TypeDefinition::Primitive(PrimitiveType::I64));
    }

    #[test]
    fn define_basic_function() {
        let def = compile_app_definition(
            "\
            fn my_func(arg_one: String, arg_two: i64):\
          \n    return 5 + 11\n\
            ",
        );

        assert_eq!(def.functions.len(), 1);

        let (args, _program, public) = &def.functions["my_func"];
        let (name, atype) = &args[1];

        assert_eq!("arg_two", name);
        assert_eq!(*atype, TypeDefinition::Primitive(PrimitiveType::I64));
        assert_eq!(public, &false);
    }

    #[test]
    fn define_type() {
        let def = compile_app_definition(
            "\
            type my_type( field: String )\n\
            ",
        );

        assert_eq!(def.types.len(), 1);
    }

    #[test]
    fn define_complex_type() {
        let def = compile_app_definition(
            "\
            type my_type( address: List<Any> )\n\
            ",
        );

        assert_eq!(def.types.len(), 1);

        let (_typeid, my_type) = def.types.get("my_type").unwrap();
        let (_, field) = my_type.get("address").unwrap();

        assert_eq!(
            *field,
            TypeDefinition::List(Box::new(TypeDefinition::Primitive(PrimitiveType::Any)))
        );
    }

    #[test]
    fn compile_cryptobirds_app() {
        let def = compile_app_definition(include_str!("../../applications/cryptobirds.app"));

        assert_eq!(def.types.len(), 1);
    }
    #[test]
    fn compile_messenger_app() {
        let def = compile_app_definition(include_str!("../../applications/messenger.app"));

        assert_eq!(def.types.len(), 1);
    }
}
