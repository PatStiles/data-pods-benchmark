"""An example of constructing a profile with install and execute services. 

Instructions:
Wait for the profile instance to start, then click on the node in the topology
and choose the `shell` menu item. The install and execute services are handled
automatically during profile instantiation, with no manual intervention required.
"""

# Import the Portal object.
import argparse
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as rspec

# Describe the parameters profile script accepts
portal.context.defineParameter("nDatapods", "Number of Datapods nodes", portal.ParameterType.INTEGER, 2)

# Retrieve values the user specifies during instantiation.
params = portal.context.bindParameters()

# Create a Request object to start building the RSpec.
request = portal.context.makeRequestRSpec()
 
# Check parameter validity.
if params.nDatapods < 1 or params.nDatapods > 50:
    portal.context.reportError(portal.ParameterError("You most choose at least 1 and no more than 50 datapods Nodes.", ["nGeth"]))

# Abort Execution if there are any errors, and report them.
portal.context.verifyParameters()

Nodes = []
for i in range(params.nDatapods):
    # Add a raw PC to the request.
    DataPodsNode = request.RawPC("Datapods" + str(i))
    Nodes.append(DataPodsNode)
    iface = DataPodsNode.addInterface("if" + str(i))

    # Specify the component id and the IPv4 address
    iface.component_id = "Datapods" + str(i)
    iface.addAddress(rspec.IPv4Address("192.172.1." + str(i), "255.255.255.0")) 

    # Request that a specific image be installed on this node
    DataPodsNode.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU20-64-STD";

    # Install and execute scripts on the node. THIS TAR FILE DOES NOT ACTUALLY EXIST!
    DataPodsNode.addService(rspec.Execute(shell="bash", command="/local/repository/datapods.sh"))

portal.context.printRequestRSpec()
