#! /usr/bin/env python2.7
"""This program converts Datalog programs written in the Souffle dialect to
   Datalog programs written in the Differential Datalog dialect"""

# pylint: disable=invalid-name,missing-docstring,global-variable-not-assigned,line-too-long
import json
import gzip
import os
import argparse
import parglare # parser generator

current_namespace = None
skip_files = False
path_rename = dict()
lhs_variables = set()   # variables that show up on the lhs of the rule
relationPrefix = "" # Prefix to prepend to all relation names when they are written to .dat files
                    # This makes it possible to concatenate multiple .dat files together
bound_variables = set() # variables that have been bound
converting_head = False # True if we are converting a head clause
converting_tail = False # True if we are converting a tail clause

def var_name(ident):
    if ident == "_":
        return ident
    if ident.startswith("?"):
        ident = ident[1:]
    return "_" + ident

class Type(object):
    types = dict() # All types in the program

    """Information about a type"""
    def __init__(self, name, outputName):
        assert name is not None
        self.name = name
        self.outputName = outputName
        self.equivalentTo = None
        self.isnumber = None  # unknown

    @classmethod
    def create(cls, name, equivalentTo):
        if name != equivalentTo:
            typ = Type(name, "T" + name)
        else:
            typ = Type(name, name)
        cls.types[name] = typ
        if equivalentTo is None:
            equivalentTo = "IString"
        else:
            assert isinstance(equivalentTo, basestring)
        assert equivalentTo is not None
        typ.equivalentTo = equivalentTo
        # print "Created " + typ.name + " same as " + typ.equivalentTo
        return typ

    @classmethod
    def get(cls, name):
        result = cls.types[name]
        assert isinstance(result, cls)
        return result

    def declaration(self):
        e = Type.get(self.equivalentTo)
        return "typedef " + self.outputName + " = " + e.outputName

    def isNumber(self):
        if self.isnumber is not None:
            return self.isnumber
        if self.name == "Tnumber" or self.name == "bit<32>":
            self.isnumber = True
            return True
        if self.name == "IString":
            self.isnumber = False
            return False
        typ = Type.get(self.equivalentTo)
        result = typ.isNumber()
        self.isnumber = result
        return result

class Parameter(object):
    """Information about a parameter (of a relation or function)"""
    def __init__(self, name, typeName):
        self.name = name
        self.typeName = typeName

    def declaration(self):
        typ = Type.get(self.typeName)
        return var_name(self.name) + ":" + typ.outputName

class RelationInfo(object):
    """Represents information about a relation"""

    relations = dict() # All relations in program

    def __init__(self, name):
        self.name = "R" + name
        self.parameters = []  # list of Parameter
        # Default values
        self.isinput = False
        self.isoutput = False

    def addParameter(self, name, typ):
        param = Parameter(name, typ)
        self.parameters.append(param)

    @classmethod
    def get(cls, name):
        if name in cls.relations:
            return cls.relations.get(name)
        prefix = current_namespace + "_" if current_namespace != None else ""
        return cls.relations.get(prefix + name)

    @classmethod
    def create(cls, name):
        prefix = current_namespace + "_" if current_namespace != None else ""
        name = prefix + name
        if name in cls.relations:
            raise Exception("duplicate relation name " + name)
        ri = RelationInfo(name)
        cls.relations[name] = ri
        # print "Registered relation " + name
        return ri

    def declaration(self):
        result = ""
        if self.isinput:
            result = "input "
        if self.isoutput:
            result = "output "
        result += "relation " + self.name + "(" + ", ".join(map(lambda a: a.declaration(), self.parameters)) + ")"
        return result

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
        Type.create("IString", "IString")
        Type.create("bit<32>", "bit<32>")

        t = Type.create("number", "bit<32>")
        self.output(t.declaration())
        t = Type.create("symbol", "IString")
        self.output(t.declaration())
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
    """Given a Parse tree node this gets all the children named field form the child list fields"""
    l = getField(node, fields)
    return getList(l, field, fields)

def getParser():
    """Parse the Parglare Souffle grammar and get the parser"""
    fileName = "souffle-grammar.pg"
    directory = os.path.dirname(os.path.abspath(__file__))
    g = parglare.Grammar.from_file(directory + "/" + fileName)
    return parglare.Parser(g, build_tree=True)

def getIdentifier(node):
    """Get a field named "Identifier" from a parglare parse tree node"""
    ident = getField(node, "Identifier")
    return ident.value

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

    ri = RelationInfo.get(rel)
    ri.isinput = True

    if skip_files or preprocess:
        return

    params = ri.parameters
    converter = []
    for p in params:
        t = p.typeName
        typ = Type.get(t)
        assert isinstance(typ, Type)
        if typ.isNumber():
            converter.append(str)
        else:
            converter.append(lambda a: json.dumps(a, ensure_ascii=False))

    print "Reading", rel, "from", filename
    if os.path.isfile(filename):
        data = open(filename, "r")
    elif os.path.isfile(filename + ".gz"):
        data = gzip.open(filename + ".gz", "r")
    else:
        raise Exception("Cannot find file " + filename)

    relname = relationPrefix + ri.name
    for line in data:
        fields = line.rstrip('\n').split(separator)
        result = "insert " + relname + "("
        for i in range(len(fields)):
            if i > 0:
                result += ", "
            result += converter[i](fields[i])
        result += ")"
        files.outputData(result, ",")
    data.close()

def process_output(outputdecl, files, preprocess):
    rel = getIdentifier(outputdecl)
    ri = RelationInfo.get(rel)
    ri.isoutput = True

    if skip_files or preprocess:
        return

def process_namespace(namespace, files, preprocess):
    global current_namespace
    if current_namespace != None:
        raise Exception("Nested namespaces: " + current_namespace + "." + namespace)
    current_namespace = getIdentifier(namespace)
    process(namespace, files, preprocess)
    current_namespace = None

###########################

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
    rel = RelationInfo.get(name)
    return rel.name + "(" + ", ".join(args_strings) + ")"

def convert_path(path):
    components = getList(path, "Identifier", "Path")
    names = map(lambda x: path_rename[x.value] if x.value in path_rename else x.value, components)
    return "_".join(names)

def convert_relation(relation, negated):
    path = getField(relation, "Path")
    cp = convert_path(path)
    rel = RelationInfo.get(cp)
    if rel is None:
        rn = cp
        # This is actually a function call
        if rn == "match":
            # Match is reserved keyword in DDlog
            rn = "re_match"
    else:
        rn = rel.name
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

    expr = getOptField(agg, "Expression")
    call = None
    if expr is not None:
        call = convert_expression(expr)
    else:
        rules = getField(agg, "ConjunctionsOrDisjunctions")
        # TODO: this only supports conjunction
        call = convert_conjunction(rules)

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
            ri = RelationInfo.get(name)
            ri.isInput = True
    else:
        heads = map(convert_head_clause, headClauses)
        converting_tail = True
        convertedTails = map(convert_conjunction, tails)
        converting_tail = False
        for ct in convertedTails:
            files.output(",\n\t".join(heads) + " :- " + ct + ".")

def process_relation_decl(relationdecl, files, preprocess):
    """Process a relation declaration and emit output to files"""
    ident = getIdentifier(relationdecl)
    params = getListField(relationdecl, "Parameter", "ParameterList")
    if preprocess:
        rel = RelationInfo.create(ident)
        if getOptField(getField(relationdecl, "OUTPUT_DEPRECATED_opt"), "OUTPUT_DEPRECATED"):
            rel.isoutput = True
        for param in params:
            arr = getArray(param, "Identifier")
            rel.addParameter(arr[0].value, arr[1].value)
        return rel

    rel = RelationInfo.get(ident)
    files.output(rel.declaration())

def process_fact_decl(factdecl, files, preprocess):
    """Process a fact"""
    if preprocess:
        return
    clause = getField(factdecl, "Clause")
    head = convert_head_clause(clause)
    files.output(head + ".")

def process_type(typedecl, files, preprocess):
    ident = getIdentifier(typedecl)
    if preprocess:
        l = getOptField(typedecl, "TypeList")
        equiv = None
        if l is not None:
            # If we have a list (a union type) we expect all members
            # of the list to be really equivalent types, so we only
            # get the first one.
            equiv = getIdentifier(l)
        typ = Type.create(ident, equiv)
        return typ
    typ = Type.get(ident)
    files.output(typ.declaration())
    return typ

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
    argParser = argparse.ArgumentParser("souffle-converter.py",
                                        description="Converts programs from Souffle Datalog into DDlog")
    argParser.add_argument("-p", "--prefix", help="Prefix to add to relations written in .dat files")
    argParser.add_argument("input", help="input Souffle program", type=str)
    argParser.add_argument("outdl", help="output DDlog program", type=str)
    argParser.add_argument("outdat", help="output DDlog data file", type=str)
    argParser.add_argument("log", nargs="?", default="/dev/null", help="output log file", type=str)
    args = argParser.parse_args()
    inputName, outputName, outputDataName, logName = args.input, args.outdl, args.outdat, args.log
    global relationPrefix
    relationPrefix = args.prefix if args.prefix else ""
    files = Files(inputName, outputName, outputDataName, logName)
    parser = getParser()
    tree = parser.parse(files.inputFile.read())
    process(tree, files, True)
    process(tree, files, False)
    files.done()

if __name__ == "__main__":
    main()
