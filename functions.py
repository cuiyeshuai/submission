import json
from collections import Counter
from prov.model import ProvDocument,QualifiedName,Namespace

PROV_ENTITY= "entity"
PROV_ACTIVITY= "activity"
PROV_GENERATION= "wasGeneratedBy"
PROV_USAGE= "used"
PROV_COMMUNICATION= "wasInformedBy"
PROV_START= "wasStartedBy"
PROV_END= "wasEndedBy"
PROV_INVALIDATION= "wasInvalidatedBy"
PROV_DERIVATION= "wasDerivedFrom"
PROV_AGENT= "agent"
PROV_ATTRIBUTION= "wasAttributedTo"
PROV_ASSOCIATION= "wasAssociatedWith"
PROV_DELEGATION= "actedOnBehalfOf"
PROV_INFLUENCE= "wasInfluencedBy"
PROV_ALTERNATE= "alternateOf"
PROV_SPECIALIZATION= "specializationOf"
PROV_MENTION= "mentionOf"
PROV_MEMBERSHIP= "hadMember"
PROV_BUNDLE= "bundle"

PROV_N_MAP = {
    "Entity": PROV_ENTITY,
    "Activity": PROV_ACTIVITY,
    "Generation": PROV_GENERATION,
    "Usage": PROV_USAGE,
    "Communication": PROV_COMMUNICATION,
    "Start": PROV_START,
    "End": PROV_END,
    "Invalidation": PROV_INVALIDATION,
    "Derivation": PROV_DERIVATION,
    "Agent": PROV_AGENT,
    "Attribution": PROV_ATTRIBUTION,
    "Association": PROV_ASSOCIATION,
    "Delegation": PROV_DELEGATION,
    "Influence": PROV_INFLUENCE,
    "Alternate": PROV_ALTERNATE,
    "Specialization": PROV_SPECIALIZATION,
    "Membership": PROV_MEMBERSHIP,
    "Bundle": PROV_BUNDLE,
}

PROV_NODE = [
    PROV_ENTITY,
    PROV_ACTIVITY,
    PROV_AGENT
]

PROV_EDGE = [
    PROV_GENERATION,
    PROV_USAGE,
    PROV_COMMUNICATION,
    PROV_START,
    PROV_END,
    PROV_INVALIDATION,
    PROV_DERIVATION,
    PROV_ATTRIBUTION,
    PROV_ASSOCIATION,
    PROV_DELEGATION,
    PROV_INFLUENCE,
    PROV_ALTERNATE,
    PROV_SPECIALIZATION,
    PROV_MEMBERSHIP
]

RELATION_MAP = {
    PROV_GENERATION: ("entity", "activity"),
    PROV_USAGE: ("activity", "entity"),
    PROV_COMMUNICATION: ("informed", "informant"),
    PROV_START: ("activity", "entity"),
    PROV_END: ("activity", "entity"),
    PROV_INVALIDATION: ("entity", "activity"),
    PROV_DERIVATION: ("generatedEntity", "usedEntity"), 
    PROV_ATTRIBUTION: ("entity", "agent"),
    PROV_ASSOCIATION: ("activity", "agent"), 
    PROV_DELEGATION: ("delegate", "responsible"),
    PROV_INFLUENCE: ("influencee", "influencer"),
    PROV_ALTERNATE: ("prov:alternate1", "prov:alternate2"),
    PROV_SPECIALIZATION: ("specificEntity", "generalEntity"),
    PROV_MEMBERSHIP: ("collection", "entity")
}

def document_to_encoding(x: ProvDocument, iri, forward):
    output = ({}, {})  # (nodes, edges)
    records = x.get_records()
    for record in records:
        if record.is_element(): # PROV-DM Type
            # element will be encoded into id -> (generic_type, specific_type)
            specific_labels = []
            for assert_type in record.get_asserted_types():
                if type(assert_type) is QualifiedName:
                    specific_labels.append(assert_type.uri) if iri else specific_labels.append(assert_type.__str__())
            res = (PROV_N_MAP[record.get_type().localpart], # generic labels
                     frozenset(specific_labels))
            output[0][record.identifier.__str__()] = res
        else:
            # relations will be encoded into node1 -> [(node2, relation_types)]
            rec_type = PROV_N_MAP.get(record.get_type().localpart)
            if rec_type is None:
                continue
            attributes = {}  # name -> value
            for attribute in record.formal_attributes:
                attributes[attribute[0].__str__()] = attribute[1].__str__()
            relation = RELATION_MAP[rec_type]
            # get starting and ending node of this relation(edge)
            edge = attributes.get("prov:" + relation[0]), attributes.get("prov:" + relation[1])

            specific_labels = []
            for assert_type in record.get_asserted_types():
                if type(assert_type) is QualifiedName:
                    specific_labels.append(assert_type.uri) if iri else specific_labels.append(assert_type.__str__())

            if None in edge:
                types_in_str = {assert_type.__str__() for assert_type in record.get_asserted_types()}
                if rec_type == PROV_DERIVATION:
                    if "prov:Revision" or "prov:Quotation" or "prov:PrimarySource" in types_in_str:
                        edge = attributes.get("prov:generatedEntity"), attributes.get("prov:usedEntity")
                elif rec_type == PROV_ASSOCIATION:
                    edge = attributes.get("prov:activity"), attributes.get("prov:plan")
            if None in edge:
                continue
            # get all relation types of this relation
            res = (rec_type, frozenset(specific_labels)) 
            if forward:
                if output[1].get(edge[0]) == None:  # if start_node is not already in relations
                    output[1][edge[0]] = [(edge[1], res)]
                else:
                    output[1][edge[0]].append((edge[1], res))
            else:
                if output[1].get(edge[1]) == None:  # if end_node is not already in relations
                    output[1][edge[1]] = [(edge[0], res)]
                else:
                    output[1][edge[1]].append((edge[0], res))
    return output


def json_to_encoding(x: str, iri, forward, qualified_names):
    output = ({}, {})  # (contents, relations)
    file = json.decoder.JSONDecoder().decode(x)
    if "prefix" in file:
        if iri:
            prefix = file["prefix"]
            prefix["prov"] = "http://www.w3.org/ns/prov#"
        del file["prefix"]
    if "bundle" in file:
        del file["bundle"]
    for rec_type in file:
        # file is dict
        # rec_type is str
        for rec_id, contents in file[rec_type].items():
            if type(contents) is dict:  # it is a dict
                #  There is only one element, create a singleton list
                contents = [contents]       
            for content in contents:
                res, specific_labels = None, frozenset()
                if rec_type in PROV_NODE: 
                    if "prov:type" in content:
                        if isinstance(content.get("prov:type"), dict):
                            content["prov:type"] = [content["prov:type"]] # make it a list
                        specific_labels = frozenset([assert_type.get("$") for assert_type in content["prov:type"] 
                            if type(assert_type) is dict and assert_type.get("type") in qualified_names])
                        if iri:
                            specific_labels = frozenset([prefix[label.split(":", 1)[0]] + label.split(":", 1)[1]
                                for label in specific_labels if label.split(":", 1)[0] in prefix])
                    output[0][rec_id] = (rec_type, specific_labels)
                else:
                    relation = RELATION_MAP.get(rec_type)
                    if relation is None:
                        continue
                    edge = content.get("prov:" + relation[0]), content.get("prov:" + relation[1])
                    if "prov:type" in content:
                        if isinstance(content.get("prov:type"), dict):
                            content["prov:type"] = [content["prov:type"]]
                        specific_labels = frozenset([assert_type.get("$") for assert_type in content["prov:type"]
                            if type(assert_type) is dict and assert_type.get("type") in qualified_names])
                    if None in edge:
                        if rec_type == PROV_DERIVATION:
                            if "prov:Revision" or "prov:Quotation" or "prov:PrimarySource" in specific_labels:
                                edge = content.get("prov:generatedEntity"), content.get("prov:usedEntity")
                        elif rec_type == PROV_ASSOCIATION:
                            edge = (content.get("prov:activity"), content.get("prov:plan"))
                    if None in edge:
                        continue
                    if iri:
                        specific_labels = frozenset([prefix[label.split(":", 1)[0]] + label.split(":", 1)[1]
                            for label in specific_labels if label.split(":", 1)[0] in prefix])
                    res = (rec_type, specific_labels)
                    if forward:
                        if output[1].get(edge[0]) == None:  # if start_node is not already in relations
                            output[1][edge[0]] = [(edge[1], res)]
                        else:
                            output[1][edge[0]].append((edge[1], res))
                    else:
                        if output[1].get(edge[1]) == None:  # if end_node is not already in relations
                            output[1][edge[1]] = [(edge[0], res)]
                        else:
                            output[1][edge[1]].append((edge[0], res))
    return output

def type_generate(x, level, specific_labels_node, specific_labels_edge):
    zero_types = {}
    for node in x[0]:
        zero_types[node] = frozenset((x[0][node],)) if specific_labels_node else frozenset((x[0][node][0],))
    prov_types = {} # prov_types up to level h
    for i in range(level + 1):
        prov_types[i] = {}
    prov_types[0] = {node: (zero_types[node],) for node in zero_types}
    for i in range(1, level+1):
        for source in x[1]: #iterate through all edges
            for destination, edge_type in x[1][source]:
                if destination in prov_types[i-1]: #if the destination is in the previous level
                    if prov_types[i].get(source) is None:
                        prov_types[i][source] = ((frozenset((edge_type,)),) if specific_labels_edge 
                            else (frozenset((edge_type[0],)),)) + prov_types[i-1][destination] 
                    else: 
                        prov_types[i][source] = tuple(m|n for m, n in zip(prov_types[i][source], ((frozenset((edge_type,)),) 
                            if specific_labels_edge else (frozenset((edge_type[0],)),)) + prov_types[i-1][destination])) 
    return prov_types

def type_generate_mixed(x, level, specific_labels_node, specific_labels_edge):
    zero_types = {}
    for node in x[0]:
        zero_types[node] = frozenset(x[0][node]) if specific_labels_node else frozenset((x[0][node][0],))
    prov_types = {} # prov_types up to level h
    for i in range(level + 1):
        prov_types[i] = {}
    prov_types[0] = {node: (zero_types[node],) for node in zero_types}
    for i in range(1, level+1):
        for source in x[1]: #iterate through all edges
            for destination, edge_type in x[1][source]:
                if destination in prov_types[i-1]: #if the destination is in the previous level
                    if prov_types[i].get(source) is None:
                        prov_types[i][source] = ((frozenset(edge_type),) 
                            if specific_labels_edge else (frozenset((edge_type[0],)),)) + prov_types[i-1][destination] 
                    else: 
                        prov_types[i][source] = tuple(m|n for m, n in zip(prov_types[i][source], ((frozenset(edge_type),) 
                            if specific_labels_edge else (frozenset((edge_type[0],)),)) + prov_types[i-1][destination])) 
    return prov_types

def type_generate_R(x, level, specific_labels_node, specific_labels_edge):
    zero_types = {}
    for node in x[0]:
        zero_types[node] = frozenset((x[0][node],)) if specific_labels_node else frozenset((x[0][node][0],))
    prov_types = {} # prov_types up to level h
    for i in range(level + 1):
        prov_types[i] = {}
    prov_types[0] = {node: (zero_types[node],) for node in zero_types}
    for i in range(1, level+1):
        for destination in prov_types[i-1]: # All nodes with prov_types of level i-1
            if destination in x[1]: # if the node is the destination of any edge
                for source, edge_type in x[1][destination]:
                    if prov_types[i].get(source) is None:
                        prov_types[i][source] = ((frozenset((edge_type,)),) if specific_labels_edge 
                            else (frozenset((edge_type[0],)),)) + prov_types[i-1][destination] 
                    else: 
                        prov_types[i][source] = tuple(m|n for m, n in zip(prov_types[i][source], ((frozenset((edge_type,)),) 
                            if specific_labels_edge else (frozenset((edge_type[0],)),)) + prov_types[i-1][destination])) 
    return prov_types

def count_prov_types(level, prov_types):
    res = dict()  # Dict[prov_type, occurence]
    for h in range(level + 1):
        res.update(dict(Counter(prov_types[h].values())))
    return res

def sparse_matrix(x, len_types, index_map):
    res = [0] * len_types
    for key in x:
        res[index_map[key]] = x[key]
    return res

# if Reverse is true then output will be features with most positive contribution
def most_important_features(x, reverse_index_map, reverse=True):
    feature_weight = [(x[i],i) for i in range(len(x))]
    feature_weight.sort(reverse=reverse)
    return [(reverse_index_map[i[1]], i[0]) for i in feature_weight]