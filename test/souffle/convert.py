#! /usr/bin/env python
"""This program converts Datalog programs written in the Souffle dialect to
   Datalog programs written in the Differential Datalog dialect"""

import parglare # parser generator

skip_files = False
current_namespace = None
relation_names = set()
path_rename = dict()

class Files(object):
    """Represents the files that are used for input and output"""

    def __init__(self):
        inputName = "self-contained.dl"
        outputName = "souffle.dl"
        outputDataName = "souffle.dat"
        logName = "souffle.log"
        self.logFile = open(logName, 'w')
        self.inputFile = open(inputName, 'r')
        self.outFile = open(outputName, 'w')
        self.output("typedef number = bigint")
        self.output("typedef symbol = string")
        self.output("function cat(s: string, t: string): string = s ++ t")
        self.outputDataFile = open(outputDataName, 'w')
        self.outputData("begin")
        print "Reading from", inputName, "writing output to", outputName, "writing data to", outputDataName

    def log(self, text):
        self.logFile.write(text + "\n")

    def output(self, text):
        self.outFile.write(text + "\n")

    def outputData(self, text):
        self.outputDataFile.write(text + ";\n")

    def done(self):
        self.outputData("commit")
        self.logFile.close()
        self.inputFile.close()
        self.outFile.close()
        self.outputDataFile.close()

# The next few functions manipulate Parglare Parse Trees
def getOptField(node, field):
    return next((x for x in node.children if x.symbol.name == field), None)

def getField(node, field):
    return next(x for x in node.children if x.symbol.name == field)

def getList(node, field, fields):
    if len(node.children) == 0:
        return []
    else:
        f = getField(node, field)
        tail = getOptField(node, fields)
        if tail == None:
            return [f]
        else:
            fs = getList(tail, field, fields)
            return [f] + fs

def getArray(node, field):
    return [x for x in node.children if x.symbol.name == field]

def getListField(node, field, fields):
    """Given a Parse tree node node this gets all the children named field form the child list fields"""
    list = getField(node, fields)
    return getList(list, field, fields)

def getParser():
    fileName = "convert.pg"
    g = parglare.Grammar.from_file(fileName)
    return parglare.Parser(g, build_tree=True)

def parse(parser, text):
    return parser.parse(text)

def strip_quotes(string):
    return string[1:-1].decode('string_escape')

def process_input(inputdecl, files):
    if skip_files:
        return
    relationname = getField(inputdecl, "Identifier")
    strings = getArray(inputdecl, "String")
    filename = strip_quotes(strings[0].value)
    separator = strip_quotes(strings[1].value)
    print "Reading", relationname.value, "from", filename
    data = open(filename, "r")
    for line in data:
        fields = line.rstrip('\n').split(separator)
        fields = map(lambda a: "[|" + a + "|]", fields)
        files.outputData("insert " + relationname.value + "(" + ", ".join(fields) + ")")
    data.close()

def process_namespace(namespace, files):
    global current_namespace
    if current_namespace != None:
        raise Exception("Nested namespaces: " + current_namespace + "." + namespace)
    current_namespace = getIdentifier(namespace)
    process(namespace, files)
    current_namespace = None

def getIdentifier(node):
    id = getField(node, "Identifier")
    return id.value

def register_relation(identifier):
    prefix = current_namespace + "_" if current_namespace != None else ""
    name = "R" + prefix + identifier;
    if name in relation_names:
        raise Exception("duplicate relation name " + name)
    relation_names.add(name)
    # print "Registered relation " + name
    return name

def relation_name(identifier):
    name = "R" + identifier
    if name in relation_names:
        return name
    prefix = current_namespace + "_" if current_namespace != None else ""
    name = "R" + prefix + identifier
    if name in relation_names:
        return name
    # Declarations can be out of order
    return name

def get_qidentifier(node):
    q = getField(node, "QIdentifier")
    return getIdentifier(q)

def var_name(id):
    if id == "_":
        return id
    return "_" + id

def process_arg(clauseArg):
    varName = getOptField(clauseArg, "VarName")
    string = getOptField(clauseArg, "String")
    if string != None:
        return string.value
    if varName != None:
        return var_name(get_qidentifier(varName))
    raise Exception("Unexpected clause argument " + clauseArg.tree_str())

def process_head_clause(clause):
    name = getField(clause, "Identifier")
    args = getListField(clause, "ClauseArg", "ClauseArgList")
    args_strings = map(process_arg, args)
    return relation_name(name.value) + "(" + ", ".join(args_strings) + ")"

def convert_path(path):
    components = getList(path, "Identifier", "Path")
    names = map(lambda x: path_rename[x.value] if x.value in path_rename else x.value, components)
    return "_".join(names)

def convert_relation(relation, negated):
    path = getField(relation, "Path")
    cp = relation_name(convert_path(path))
    args = getListField(relation, "ClauseArg", "ClauseArgList")
    args_strings = map(process_arg, args)
    if negated:
        cp = "not " + cp
    return cp + "(" + ", ".join(args_strings) + ")"

def convert_function_argument(arg):
    id = getOptField(arg, "QIdentifier")
    if id != None:
        return var_name(getIdentifier(id))
    str = getField(arg, "String")
    return str.value

def convert_function(function):
    id = getField(function, "Identifier")
    args = getListField(function, "FunctionArgument", "FunctionArgumentList")
    argStrings = map(convert_function_argument, args)
    return id.value + "(" + ", ".join(argStrings) + ")"

def convert_assignment(assign):
    idents = getArray(assign, "QIdentifier")
    id0 = getIdentifier(idents[0])
    func = getOptField(assign, "FunctionCall")
    strg = getOptField(assign, "String")
    prefix = "var " + var_name(id0) + " = ";
    if strg != None:
        return prefix + strg.value
    if func != None:
        return prefix + convert_function(func)
    id1 = idents[1].value
    return prefix + id1

def convert_expression(expr):
    assign = getOptField(expr, "Assignment")
    if assign != None:
        return convert_assignment(assign)
    negation = getOptField(expr, "!")
    relation = getOptField(expr, "Relation")
    if relation != None:
        return convert_relation(relation, negation != None)
    ne = getOptField(expr, "NE")
    if ne != None:
        idents = getArray(expr, "QIdentifier")
        id0 = getIdentifier(idents[0])
        strg = getOptField(expr, "String")
        if strg == None:
            id1 = getIdentifier(idents[1])
        else:
            id1 = strg.value
        return "not (" + id0 + " = " + id1 + ")"
    raise Exception("Unexpected expression" + expr.tree_str())

def process_conjunction(conj):
    expr = getOptField(conj, "Expression")
    if expr != None:
        return convert_expression(expr)
    children = getArray(conj, "ConjunctionsOrDisjunctions")
    operator = getOptField(conj, "OR")
    assert operator == None
    rec = map(process_conjunction, children)
    return ", ".join(rec)

def normalize_tail(tail):
    """Converts a tail into a disjunction of conjunctions.
       Returns a list with all disjunctions"""
    # TODO
    return [getField(tail, "ConjunctionsOrDisjunctions")]

def process_rule(rule, files):
    head = getField(rule, "Head")
    tail = getField(rule, "Tail")
    headClauses = getListField(head, "Clause", "ClauseList")
    heads = map(process_head_clause, headClauses)
    tails = normalize_tail(tail)
    convertedTails = map(process_conjunction, tails)
    for ct in convertedTails:
        files.output(",\n\t".join(heads) + " :- " + ct + ".")

def process_decl_param(param):
    type = getField(param, "Identifier")
    return var_name(get_qidentifier(param)) + ": " + type.value

def process_relation_decl(relationdecl, files):
    id = getField(relationdecl, "Identifier")
    params = getListField(relationdecl, "Parameter", "ParameterList")
    paramdecls = map(process_decl_param, params)
    files.output("relation " + register_relation(id.value) + "(" + ", ".join(paramdecls) + ")")

def process_decl(decl, files):
    files.log(decl.tree_str())
    typedecl = getOptField(decl, "TypeDecl")
    if typedecl != None:
        id = getField(typedecl, "Identifier")
        files.output("typedef " + id.value + " = string")
        return
    relationdecl = getOptField(decl, "RelationDecl")
    if relationdecl != None:
        process_relation_decl(relationdecl, files)
        return
    inputdecl = getOptField(decl, "InputDecl")
    if inputdecl != None:
        process_input(inputdecl, files)
        return
    rule = getOptField(decl, "Rule")
    if rule != None:
        process_rule(rule, files)
        return
    namespace = getOptField(decl, "Namespace")
    if namespace != None:
        process_namespace(namespace, files)
        return
    init = getOptField(decl, "Init")
    if init != None:
        ids = getArray(init, "Identifier")
        path_rename[ids[0].value] = ids[1].value
        return
    raise Exception("Unexpected node " + decl.tree_str())

def process(tree, files):
    decls = getListField(tree, "Declaration", "DeclarationList")
    for decl in decls:
        process_decl(decl, files)

def main():
    files = Files()
    parser = getParser()
    input = ""
    tree = parser.parse(files.inputFile.read())
    process(tree, files)
    files.done()

if __name__ == "__main__":
    main()
