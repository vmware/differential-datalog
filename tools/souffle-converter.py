#! /usr/bin/env python2.7
"""This program converts Datalog programs written in the Souffle dialect to
   Datalog programs written in the Differential Datalog dialect"""

# pylint: disable=invalid-name,missing-docstring,global-variable-not-assigned,line-too-long
import json
import gzip
import os
import argparse
import parglare # parser generator

skip_files = False
current_namespace = None
relations = dict()  # Maps relation to bool indicating whether the relation is an input
outrelations = dict()  # Maps relation to bool indicating whether the relation is an output
path_rename = dict()
lhs_variables = set()   # variables that show up on the lhs of the rule
bound_variables = set() # variables that have been bound
converting_head = False # True if we are converting a head clause
converting_tail = False # True if we are converting a tail clause

class Files(object):
    """Represents the files that are used for input and output"""

    def __init__(self, inputDl, outputDl, outputDat, log):
        inputName = inputDl
        outputName = outputDl
        outputDataName = outputDat
        logName = log
        self.logFile = open(logName, 'w')
        self.inputFile = open(inputName, 'r')
        self.outFile = open(outputName, 'w')
        self.output("import intern")
        self.output("import souffle_lib")
        self.output("typedef number = IString")
        self.output("typedef symbol = IString")
        self.outputDataFile = open(outputDataName, 'w')
        self.outputData("echo Reading data", ";")
        #self.outputData("timestamp", ";")
        self.outputData("start", ";")
        print "Reading from", inputName, "writing output to", outputName, "writing data to", outputDataName

    def log(self, text):
        self.logFile.write(text + "\n")

    def output(self, text):
        self.outFile.write(text + "\n")

    def outputData(self, text, terminator):
        self.outputDataFile.write(text + terminator + "\n")

    def done(self):
        self.outputData("echo Finished adding data, committing", ";")
        #self.outputData("timestamp", ";")
        self.outputData("commit", ";")
        #self.outputData("timestamp", ";")
        #self.outputData("profile", ";")
        self.outputData("dump", ";")
        self.outputData("echo done", ";")
        self.outputData("exit", ";")
        self.logFile.close()
        self.inputFile.close()
        self.outFile.close()
        self.outputDataFile.close()

# The next few functions manipulate Parglare Parse Trees
def getOptField(node, field):
    """Returns the first field named 'field' from 'node', or None if it does not exist"""
    return next((x for x in node.children if x.symbol.name == field), None)

def getField(node, field):
    """Returns the first field named 'field' from 'node'"""
    return next(x for x in node.children if x.symbol.name == field)

def getList(node, field, fields):
    if len(node.children) == 0:
        return []
    else:
        f = getField(node, field)
        tail = getOptField(node, fields)
        if tail is None:
            return [f]
        else:
            fs = getList(tail, field, fields)
            return [f] + fs

def getArray(node, field):
    return [x for x in node.children if x.symbol.name == field]

def getListField(node, field, fields):
    """Given a Parse tree node node this gets all the children named field form the child list fields"""
    l = getField(node, fields)
    return getList(l, field, fields)

def getParser():
    fileName = "souffle-grammar.pg"
    directory = os.path.dirname(os.path.abspath(__file__))
    g = parglare.Grammar.from_file(directory + "/" + fileName)
    return parglare.Parser(g, build_tree=True)

##############################################################

def parse(parser, text):
    return parser.parse(text)

def strip_quotes(string):
    return string[1:-1].decode('string_escape')

def process_input(inputdecl, files, preprocess):
    rel = getIdentifier(inputdecl)
    strings = getArray(inputdecl, "String")
    if len(strings) == 0:
        filename = rel + ".facts"
        separator = ','
    else:
        filename = strip_quotes(strings[0].value)
        separator = strip_quotes(strings[1].value)

    relationname = relation_name(rel)
    global relations
    relations[relationname] = True  # Input relation

    if skip_files or preprocess:
        return

    print "Reading", rel, "from", filename
    if os.path.isfile(filename):
        data = open(filename, "r")
    elif os.path.isfile(filename + ".gz"):
        data = gzip.open(filename + ".gz", "r")
    else:
        raise Exception("Cannot find file " + filename)

    for line in data:
        fields = line.rstrip('\n').split(separator)
        fields = map(lambda a: json.dumps(a, ensure_ascii=False), fields)
        files.outputData("insert " + relationname + "(" + ", ".join(fields) + ")", ",")
    data.close()

def process_output(outputdecl, files, preprocess):
    rel = getIdentifier(outputdecl)
    relationname = relation_name(rel)
    outrelations[relationname] = True  # Output relation

    if skip_files or preprocess:
        return

def process_namespace(namespace, files, preprocess):
    global current_namespace
    if current_namespace != None:
        raise Exception("Nested namespaces: " + current_namespace + "." + namespace)
    current_namespace = getIdentifier(namespace)
    process(namespace, files, preprocess)
    current_namespace = None

def getIdentifier(node):
    ident = getField(node, "Identifier")
    return ident.value

def register_relation(identifier):
    global relations
    prefix = current_namespace + "_" if current_namespace != None else ""
    name = "R" + prefix + identifier
    if name in relations:
        raise Exception("duplicate relation name " + name)
    relations[name] = False
    outrelations[name] = False
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

def is_output_relation(identifier):
    name = relation_name(identifier)
    return outrelations[name]

def var_name(ident):
    if ident == "_":
        return ident
    if ident.startswith("?"):
        ident = ident[1:]
    return "_" + ident

def convert_arg(clauseArg):
    varName = getOptField(clauseArg, "VarName")
    string = getOptField(clauseArg, "String")
    func = getOptField(clauseArg, "FunctionCall")
    if string != None:
        return "string_intern(" + string.value + ")"
    if varName != None:
        v = var_name(getIdentifier(varName))
        if converting_head and v != "_":
            lhs_variables.add(v)
        if converting_tail and v != "_":
            bound_variables.add(v)
        return v
    if func != None:
        return convert_function_call(func)
    # TODO: handle functor call
    raise Exception("Unexpected clause argument " + clauseArg.tree_str())

def convert_head_clause(clause):
    global converting_head
    converting_head = True
    name = getIdentifier(clause)
    args = getListField(clause, "ClauseArg", "ClauseArgList")
    args_strings = map(convert_arg, args)
    converting_head = False
    return relation_name(name) + "(" + ", ".join(args_strings) + ")"

def convert_path(path):
    components = getList(path, "Identifier", "Path")
    names = map(lambda x: path_rename[x.value] if x.value in path_rename else x.value, components)
    return "_".join(names)

def convert_relation(relation, negated):
    path = getField(relation, "Path")
    cp = convert_path(path)
    rn = relation_name(cp)
    if rn not in relations:
        rn = cp
    args = getListField(relation, "ClauseArg", "ClauseArgList")
    args_strings = map(convert_arg, args)
    if negated:
        rn = "not " + rn
    return rn + "(" + ", ".join(args_strings) + ")"

def convert_function_argument(arg):
    ident = getOptField(arg, "Identifier")
    if ident != None:
        return var_name(ident.value)

    s = getField(arg, "String")
    if s != None:
        return s.value

    fc = getField(arg, "FunctionCall")
    if fc != None:
        func = getOptField(arg, "FunctionCall")
        str_cf = convert_function_call(func)
        return str_cf
    raise Exception("Unexpected function argument" + arg.tree_str())

def convert_function_call(function):
    ident = getIdentifier(function)
    args = getListField(function, "FunctionArgument", "FunctionArgumentList")
    argStrings = map(convert_function_argument, args)
    if ident == "match":
        # Match is reserved keyword in DDlog
        ident = "re_match"
    return ident + "(" + ", ".join(argStrings) + ")"

def convert_aggregate(agg):
    """Convert an aggregate call; returns a list with two elements:
       A set of expressions to evaluate before aggregation, and the
       aggregation proper"""
    # Must process the bound variables before the expression
    result = "Aggregate(("
    result += ", ".join(bound_variables)
    result += "), "

    call = convert_expression(getField(agg, "Expression"))
    ident = getIdentifier(agg)
    func = getField(agg, "AggregateFunction")
    result += "group_" + func.children[0].value + "(" + var_name(ident) + "))"
    return [call, result]

def convert_assignment(assign):
    idents = getArray(assign, "Identifier")
    id0 = idents[0].value
    prefix = "var " + var_name(id0) + " = "

    strg = getOptField(assign, "String")
    if strg != None:
        return prefix + "string_intern(" + strg.value + ")"

    func = getOptField(assign, "FunctionCall")
    if func != None:
        return prefix + convert_function_call(func)

    agg = getOptField(assign, "Aggregate")
    if agg != None:
        [before, after] = convert_aggregate(agg)
        return before + ", " + prefix + after

    if len(idents) == 2:
        id1 = idents[1].value
        return prefix + id1

    raise Exception("Unexpected assignment" + assign.tree_str())

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
        idents = getArray(expr, "Identifier")
        id0 = var_name(idents[0].value)
        strg = getOptField(expr, "String")
        if strg is None:
            id1 = var_name(idents[1].value)
        else:
            id1 = "string_intern(" + strg.value + ")"
        return "not (" + id0 + " == " + id1 + ")"
    raise Exception("Unexpected expression" + expr.tree_str())

def convert_conjunction(conj):
    """Convert a conjunction of expressions into a string"""
    expr = getOptField(conj, "Expression")
    if expr != None:
        return convert_expression(expr)
    children = getArray(conj, "ConjunctionsOrDisjunctions")
    operator = getOptField(conj, "OR")
    assert operator is None, "Disjunction not yet implemented"
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
    return reduce(lambda a, b: a or b, rec, False)

def process_rule(rule, files, preprocess):
    """Convert a rule and emit the output"""
    global converting_tail
    global lhs_variables

    head = getField(rule, "Head")
    tail = getField(rule, "Tail")
    lhs_variables.clear()
    headClauses = getListField(head, "Clause", "ClauseList")
    tails = normalize_tail(tail)

    if not has_relations(tail) and preprocess:
        # If there are no clauses in the tail we
        # mark all input relations as input relations
        for clause in headClauses:
            name = getIdentifier(clause)
            relations[relation_name(name)] = True
    else:
        heads = map(convert_head_clause, headClauses)
        converting_tail = True
        convertedTails = map(convert_conjunction, tails)
        converting_tail = False
        for ct in convertedTails:
            files.output(",\n\t".join(heads) + " :- " + ct + ".")

def convert_decl_param(param):
    """Convert a declaration parameter and return the corresponding string"""
    arr = getArray(param, "Identifier")
    arg = arr[0].value
    type_ = arr[1].value
    return var_name(arg) + ": " + type_

def process_relation_decl(relationdecl, files, preprocess):
    """Process a relation declaration and emit output to files"""
    ident = getIdentifier(relationdecl)
    params = getListField(relationdecl, "Parameter", "ParameterList")
    if preprocess:
        relname = register_relation(ident)
        return
    relname = relation_name(ident)
    paramdecls = map(convert_decl_param, params)
    role = "input " if relations[relname] else ""
    #print(relationdecl.tree_str())
    if getOptField(getField(relationdecl, "OUTPUT_DEPRECATED_opt"), "OUTPUT_DEPRECATED") \
           != None or outrelations[relname]:
        role = "output "
    files.output(role + "relation " + relname + "(" + ", ".join(paramdecls) + ")")

def process_fact_decl(factdecl, files, preprocess):
    """Process a fact"""
    if preprocess:
        return
    clause = getField(factdecl, "Clause")
    head = convert_head_clause(clause)
    files.output(head + ".")

def process_type(typedecl, files, preprocess):
    if preprocess:
        return
    ident = getIdentifier(typedecl)
    l = getOptField(typedecl, "TypeList")
    t = "IString"
    if l != None:
        # If we have a list (a union type) we expect all members of
        # the list to be really equivalent types.
        typeValue = getIdentifier(l)
        if typeValue == "number":
            t = "bit<32>"
    files.output("typedef " + ident + " = " + t)
    return

def process_decl(decl, files, preprocess):
    """Process a declaration; dispatches on declaration kind"""
    files.log(decl.tree_str())
    typedecl = getOptField(decl, "TypeDecl")
    if typedecl != None:
        process_type(typedecl, files, preprocess)
        return
    fact_decl = getOptField(decl, "Fact")
    if fact_decl != None:
        process_fact_decl(fact_decl, files, preprocess)
        return
    relationdecl = getOptField(decl, "RelationDecl")
    if relationdecl != None:
        process_relation_decl(relationdecl, files, preprocess)
        return
    inputdecl = getOptField(decl, "InputDecl")
    if inputdecl != None:
        process_input(inputdecl, files, preprocess)
        return
    outputdecl = getOptField(decl, "OutputDecl")
    if outputdecl != None:
        process_output(outputdecl, files, preprocess)
        return
    rule = getOptField(decl, "Rule")
    if rule != None:
        if preprocess:
            return
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
    # TODO: handle functor
    raise Exception("Unexpected node " + decl.tree_str())

def process(tree, files, preprocess):
    decls = getListField(tree, "Declaration", "DeclarationList")
    for decl in decls:
        process_decl(decl, files, preprocess)

def main():
    argParser = argparse.ArgumentParser("souffle-converter.py")
    argParser.add_argument("input", help="input Souffle program", type=str)
    argParser.add_argument("outdl", help="output DDlog program", type=str)
    argParser.add_argument("outdat", help="output DDlog data file", type=str)
    argParser.add_argument("log", help="output log file", type=str)
    args = argParser.parse_args()
    inputName, outputName, outputDataName, logName = args.input, args.outdl, args.outdat, args.log
    files = Files(inputName, outputName, outputDataName, logName)
    parser = getParser()
    tree = parser.parse(files.inputFile.read())
    process(tree, files, True)
    process(tree, files, False)
    files.done()

if __name__ == "__main__":
    main()
