"""Setup for Gus replication experiments."""

import ast
# Import the Portal object.
import geni.portal as portal

# Create a portal object,
pc = portal.Context()

portal.context.defineParameter("replicas", "List of Replica Names", portal.ParameterType.STRING, "['california', 'virginia', 'ireland', 'oregon', 'japan']")
portal.context.defineParameter("replica_type", "Replica Hardware Type", portal.ParameterType.NODETYPE, "m510")
portal.context.defineParameter("client_type", "Client Hardware Type", portal.ParameterType.NODETYPE, "c6525-25g")
portal.context.defineParameter("control_machine", "Use Control Machine?", portal.ParameterType.BOOLEAN, True)
portal.context.defineParameter("control_type", "Control Hardware Type", portal.ParameterType.NODETYPE, "m510")
portal.context.defineParameter("control_disk_image", "Control Machine Disk Image", portal.ParameterType.IMAGE, "urn:publicid:IDN+utah.cloudlab.us+image+hyflowtm-PG0:Gus-redis")

params = portal.context.bindParameters()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()
replicas = ast.literal_eval(params.replicas)

lan_list = []
# Instantiate server machines.
for i in range(len(replicas)):
    replica = request.RawPC(replicas[i])
    lan_list.append(replica)
    replica.hardware_type = params.replica_type

# Instantiate client machine
client = request.RawPC('client')
lan_list.append(client)
client.hardware_type = params.client_type

# Instantiate control machine
if params.control_machine:
    control = request.RawPC('control')
    lan_list.append(control)
    control.hardware_type = params.control_type

lan = request.Link(members=lan_list)

# Print the generated rspec
pc.printRequestRSpec(request)