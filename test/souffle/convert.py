#! /usr/bin/env python
"""This program converts Datalog programs written in the Souffle dialect to
   Datalog programs written in the Differential Datalog dialect"""

import parglare # parser generator
import json
import gzip
import os

skip_files = False
current_namespace = None
relations = dict()  # Maps relation to bool indicating whether the relation is an input
path_rename = dict()

total = 0

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
        self.output("typedef number = string")
        self.output("typedef symbol = string")
        self.output("function cat(s: string, t: string): string = s ++ t")
        self.outputDataFile = open(outputDataName, 'w')
        self.outputData("start")
        print "Reading from", inputName, "writing output to", outputName, "writing data to", outputDataName

    def log(self, text):
        self.logFile.write(text + "\n")

    def output(self, text):
        self.outFile.write(text + "\n")

    def outputData(self, text):
        self.outputDataFile.write(text + ";\n")

    def done(self):
        self.outputData("echo Finished adding data")
        self.outputData("commit")
        self.outputData("echo done")
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

def process_input(inputdecl, files, preprocess):
    rel = getField(inputdecl, "Identifier")
    strings = getArray(inputdecl, "String")
    filename = strip_quotes(strings[0].value)
    separator = strip_quotes(strings[1].value)

    relationname = relation_name(rel.value)
    relations[relationname] = True  # Input relation

    if skip_files or preprocess:
        return

    print "Reading", rel.value, "from", filename
    if os.path.isfile(filename):
        data = open(filename, "r")
    elif os.path.isfile(filename + ".gz"):
        data = gzip.open(filename + ".gz", "r")
    else:
        raise Exception("Cannot find file " + filename)

    counter = 0
    global total
    for line in data:
        fields = line.rstrip('\n').split(separator)
        fields = map(lambda a: json.dumps(a), fields)
        files.outputData("insert " + relationname + "(" + ", ".join(fields) + ")")
        counter = counter + 1
        total = total + 1
        if counter == 1000:
            files.outputData("commit")
            files.outputData("profile")
            files.outputData("echo total: " + str(total))
            files.outputData("start")
            counter = 0
    data.close()

def process_namespace(namespace, files, preprocess):
    global current_namespace
    if current_namespace != None:
        raise Exception("Nested namespaces: " + current_namespace + "." + namespace)
    current_namespace = getIdentifier(namespace)
    process(namespace, files, preprocess)
    current_namespace = None

def getIdentifier(node):
    id = getField(node, "Identifier")
    return id.value

def register_relation(identifier):
    global relations
    prefix = current_namespace + "_" if current_namespace != None else ""
    name = "R" + prefix + identifier;
    if name in relations:
        raise Exception("duplicate relation name " + name)
    relations[name] = False
    # print "Registered relation " + name
    return name

def relation_name(identifier):
    global relations
    name = "R" + identifier
    if name in relations:
        return name
    prefix = current_namespace + "_" if current_namespace != None else ""
    name = "R" + prefix + identifier
    if name in relations:
        return name
    # Declarations can be out of order
    return name

def is_input_relation(identifier):
    name = relation_name(identifier)
    return relations[name]

def get_qidentifier(node):
    q = getField(node, "QIdentifier")
    return getIdentifier(q)

def var_name(id):
    if id == "_":
        return id
    return "_" + id

def convert_arg(clauseArg):
    varName = getOptField(clauseArg, "VarName")
    string = getOptField(clauseArg, "String")
    if string != None:
        return string.value
    if varName != None:
        return var_name(get_qidentifier(varName))
    raise Exception("Unexpected clause argument " + clauseArg.tree_str())

def convert_head_clause(clause):
    name = getField(clause, "Identifier")
    args = getListField(clause, "ClauseArg", "ClauseArgList")
    args_strings = map(convert_arg, args)
    return relation_name(name.value) + "(" + ", ".join(args_strings) + ")"

def convert_path(path):
    components = getList(path, "Identifier", "Path")
    names = map(lambda x: path_rename[x.value] if x.value in path_rename else x.value, components)
    return "_".join(names)

def convert_relation(relation, negated):
    path = getField(relation, "Path")
    cp = relation_name(convert_path(path))
    args = getListField(relation, "ClauseArg", "ClauseArgList")
    args_strings = map(convert_arg, args)
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
        id0 = var_name(getIdentifier(idents[0]))
        strg = getOptField(expr, "String")
        if strg == None:
            id1 = var_name(getIdentifier(idents[1]))
        else:
            id1 = strg.value
        return "not (" + id0 + " == " + id1 + ")"
    raise Exception("Unexpected expression" + expr.tree_str())

def convert_conjunction(conj):
    """Convert a conjunction of expressions into a string"""
    expr = getOptField(conj, "Expression")
    if expr != None:
        return convert_expression(expr)
    children = getArray(conj, "ConjunctionsOrDisjunctions")
    operator = getOptField(conj, "OR")
    assert operator == None
    rec = map(convert_conjunction, children)
    return ", ".join(rec)

def normalize_tail(tail):
    """Converts a tail into a disjunction of conjunctions.
       Returns a list with all disjunctions"""
    # TODO
    return [getField(tail, "ConjunctionsOrDisjunctions")]

def expression_has_relations(expression):
    relation = getOptField(expression, "Relation")
    return relation != None

def has_relations(conj):
    """True if a conjunction contains any relations"""
    expr = getOptField(conj, "Expression")
    if expr != None:
        return expression_has_relations(expr)
    children = getArray(conj, "ConjunctionsOrDisjunctions")
    rec = map(has_relations, children)
    return reduce(lambda a,b: a or b, rec, False)

def process_rule(rule, files, preprocess):
    """Convert a rule and emit the output"""
    head = getField(rule, "Head")
    tail = getField(rule, "Tail")
    headClauses = getListField(head, "Clause", "ClauseList")
    tails = normalize_tail(tail)

    if not has_relations(tail) and preprocess:
        # If there are no clauses in the tail we
        # mark all input relations as input relations
        for clause in headClauses:
            name = getField(clause, "Identifier")
            relations[relation_name(name.value)] = True
        # TODO: we should also emit the facts as data...
    else:
        heads = map(convert_head_clause, headClauses)
        convertedTails = map(convert_conjunction, tails)
        for ct in convertedTails:
            files.output(",\n\t".join(heads) + " :- " + ct + ".")

def convert_decl_param(param):
    """Convert a declaration parameter and return the corresponding string"""
    type = getField(param, "Identifier")
    return var_name(get_qidentifier(param)) + ": " + type.value

def process_relation_decl(relationdecl, files, preprocess):
    """Process a relation declaration and emit output to files"""
    id = getField(relationdecl, "Identifier")
    params = getListField(relationdecl, "Parameter", "ParameterList")
    if preprocess:
        relname = register_relation(id.value)
        return
    relname = relation_name(id.value)
    paramdecls = map(convert_decl_param, params)
    is_input = "input " if relations[relname] else ""
    files.output(is_input + "relation " + relname + "(" + ", ".join(paramdecls) + ")")

def process_decl(decl, files, preprocess):
    files.log(decl.tree_str())
    typedecl = getOptField(decl, "TypeDecl")
    if typedecl != None:
        if preprocess:
            return
        id = getField(typedecl, "Identifier")
        files.output("typedef " + id.value + " = string")
        return
    relationdecl = getOptField(decl, "RelationDecl")
    if relationdecl != None:
        process_relation_decl(relationdecl, files, preprocess)
        return
    inputdecl = getOptField(decl, "InputDecl")
    if inputdecl != None:
        process_input(inputdecl, files, preprocess)
        return
    rule = getOptField(decl, "Rule")
    if rule != None:
        process_rule(rule, files, preprocess)
        return
    namespace = getOptField(decl, "Namespace")
    if namespace != None:
        process_namespace(namespace, files, preprocess)
        return
    init = getOptField(decl, "Init")
    if init != None:
        ids = getArray(init, "Identifier")
        path_rename[ids[0].value] = ids[1].value
        return
    raise Exception("Unexpected node " + decl.tree_str())

def process(tree, files, preprocess):
    decls = getListField(tree, "Declaration", "DeclarationList")
    for decl in decls:
        process_decl(decl, files, preprocess)

def main():
    files = Files()
    parser = getParser()
    input = ""
    tree = parser.parse(files.inputFile.read())
    process(tree, files, True)
    process(tree, files, False)
    files.done()

if __name__ == "__main__":
    main()
