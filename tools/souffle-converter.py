#! /usr/bin/env python2.7
"""This program converts Datalog programs written in the Souffle dialect to
   Datalog programs written in the Differential Datalog dialect"""

# pylint: disable=invalid-name,missing-docstring,global-variable-not-assigned,line-too-long
from __future__ import unicode_literals
import json
import gzip
import os
import argparse
import parglare # parser generator

debug = False
skip_files = False  # If true do not process .input and .output declarations
relationPrefix = "" # Prefix to prepend to all relation names when they are written to .dat files
                    # This makes it possible to concatenate multiple .dat files together

def var_name(ident):
    if ident == "_":
        return ident
    if ident.startswith("?"):
        ident = ident[1:]
    return "_" + ident

def parse(parser, text):
    return parser.parse(text)

def strip_quotes(string):
    assert string[0] == "\"", "String is not quoted"
    assert string[len(string) - 1] == "\"", "String is not quoted"
    return string[1:-1].decode('string_escape')

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

class Relation(object):
    """Represents information about a relation"""

    relations = dict() # All relations in program
    dumpOrder = []  # order in which we dump the relations

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
    def get(cls, name, component):
        if name in cls.relations:
            return cls.relations.get(name)
        prefix = component + "_" if component != None else ""
        result = cls.relations.get(prefix + name)
        assert result is not None, "Could not locate relation " + name
        return result

    @classmethod
    def create(cls, name, component):
        assert isinstance(name, basestring)
        prefix = component + "_" if component != None else ""
        name = prefix + name
        if name in cls.relations:
            raise Exception("duplicate relation name " + name)
        ri = Relation(name)
        cls.relations[name] = ri
        # print "Registered relation " + name
        return ri

    def declaration(self):
        result = ""
        if self.isinput:
            result = "input "
        if self.isoutput:
            result = "output "
        result += "relation " + self.name + "(" + ", ".join([a.declaration() for a in self.parameters]) + ")"
        return result

class Files(object):
    """Represents the files that are used for input and output"""

    def __init__(self, inputDl, outputPrefix):
        self.inputName = inputDl
        outputName = outputPrefix + ".dl"
        outputDataName = outputPrefix + ".dat"
        outputDumpName = outputPrefix + ".dump.expected"
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
        self.dumpFile = open(outputDumpName, 'w')
        self.outputData("echo Reading data", ";")
        #self.outputData("timestamp", ";")
        self.outputData("start", ";")
        print "Reading from", self.inputName, "writing output to", outputName, "writing data to", outputDataName

    def output(self, text):
        self.outFile.write(text + "\n")

    def outputData(self, text, terminator):
        self.outputDataFile.write(text + terminator + "\n")

    def done(self, dumporder):
        self.outputData("echo Finished adding data, committing", ";")
        self.outputData("commit", ";")
        if len(dumporder) == 0:
            self.outputData("dump", ";")
        else:
            for r in dumporder:
                self.outputData("dump " + r, ";")
        self.outputData("dump", ";")
        self.outputData("echo done", ";")
        self.outputData("exit", ";")
        self.outFile.close()
        self.outputDataFile.close()
        self.dumpFile.close()

# The next few functions manipulate Parglare Parse Trees
def getOptField(node, field):
    """Returns the first field named 'field' from 'node', or None if it does not exist"""
    return next((x for x in node.children if x.symbol.name == field), None)

def getField(node, field):
    """Returns the first field named 'field' from 'node'"""
    l = [x for x in node.children if x.symbol.name == field]
    if l == []:
        raise Exception("No field " + field + " in " + node.tree_str())
    return l[0]

def getList(node, field, fields):
    """Returns a list produced by a parglare *right-recursive* rule"""
    global debug
    if debug:
        print node
        for c in node.children:
            print "\t", c
        print "----"
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

def getParser(debugParser):
    """Parse the Parglare Souffle grammar and get the parser"""
    fileName = "souffle-grammar.pg"
    directory = os.path.dirname(os.path.abspath(__file__))
    g = parglare.Grammar.from_file(directory + "/" + fileName)
    return parglare.Parser(g, build_tree=True, debug=debugParser)

def getIdentifier(node):
    """Get a field named "Identifier" from a parglare parse tree node"""
    ident = getField(node, "Identifier")
    return ident.value

##############################################################

class SouffleConverter(object):
    def __init__(self, files):
        assert isinstance(files, Files)
        self.files = files
        self.preprocess = False
        self.current_component = None
        self.bound_variables = set() # variables that have been bound
        self.all_variables = set()   # all variable names that appear in the program
        self.converting_head = False # True if we are converting a head clause
        self.converting_tail = False # True if we are converting a tail clause
        self.lhs_variables = set()   # variables that show up on the lhs of the rule
        self.path_rename = dict()
        self.aggregate_prefix = ""   # Expression evaluated before an aggregate
        self.dummyRelation = None    # Relation used to convert clauses that start with a negative term
        self.opdict = {
            "band": "&",
            "bor": "|",
            "bxor": "^",
            "bnot": "~",
            "land": "&&",
            "lor": "||",
            "lnot": "!"
        }


    def fresh_variable(self, prefix):
        """Return a variable whose name does not yet occur in the program"""
        name = prefix
        suffix = 0
        while name in self.all_variables:
            name = prefix + "_" + str(suffix)
            suffix = suffix + 1
        self.all_variables.add(name)
        return name

    def process_file(self, rel, inFileName, inSeparator, outputEmitter):
        """Process an INPUT or OUTPUT with name inFileName; dump its contents into outfile
        rel is the relation name that is being processed
        inFileName is the file which contains the data
        inSeparator is the input record separator
        outputEmitter is a lambda which does the output.  It takes argument
        an array of string arguments for the relation.
        """
        ri = Relation.get(rel, self.current_component)
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

        if inFileName.endswith(".gz"):
            data = gzip.open(inFileName, "r")
        else:
            data = open(inFileName, "r")
        lineno = 0
        for line in data:
            fields = line.rstrip('\n').split(inSeparator)
            result = []
            # Special handling for the empty tuple, which seems
            # to be written as () in Souffle, instead of the emtpy string
            if len(converter) == 0 and len(fields) == 1 and fields[0] == "()":
                fields = []
            elif len(fields) != len(converter):
                raise Exception("Line " + lineno + " does not match schema (expected " + str(len(converter)) + \
                                " parameters):" + line)
            for i in range(len(fields)):
                result.append(converter[i](fields[i]))
            outputEmitter(result)
            lineno = lineno + 1
        data.close()

    @staticmethod
    def get_KVValue(KVValue):
        string = getOptField(KVValue, "String")
        if string is not None:
            return strip_quotes(string.value)
        ident = getOptField(KVValue, "Identifier")
        if ident is not None:
            return ident.value
        return "true"

    @staticmethod
    def get_kvp(keyValuePairs):
        ident = getOptField(keyValuePairs, "Identifier")
        if ident is None:
            return dict()
        kvv = getOptField(keyValuePairs, "KVValue")
        kvvalue = SouffleConverter.get_KVValue(kvv)
        rec = getOptField(keyValuePairs, "KeyValuePairs")
        if rec is not None:
            result = SouffleConverter.get_kvp(rec)
        else:
            result = dict()
        result[ident.value] = kvvalue
        return result

    def get_relid(self, decl):
        # print decl.tree_str()
        l = getListField(decl, "Identifier", "RelId")
        values = [e.value for e in l]
        renamed = [self.path_rename[x] if x in self.path_rename else x for x in values]
        return "_".join(renamed)

    def process_input(self, inputdecl):
        directives = getField(inputdecl, "IodirectiveList")
        body = getField(directives, "IodirectiveBody")
        rel = self.get_relid(body)
        kvpf = getOptField(body, "KeyValuePairs")
        if kvpf is not None:
            kvp = self.get_kvp(kvpf)
        else:
            kvp = dict()

        if ("IO" in kvp) and (kvp["IO"] != "file"):
            raise Exception("Unexpected IO source " + kvp["IO"])

        if "filename" not in kvp:
            filename = rel + ".facts"
        else:
            filename = kvp["filename"]

        if "separator" not in kvp:
            separator = '\t'
        else:
            separator = kvp["separator"]

        ri = Relation.get(rel, self.current_component)
        ri.isinput = True

        if skip_files or self.preprocess:
            return

        print "Reading", rel, "from", filename
        data = None
        for directory in ["./", "./facts/"]:
            for suffix in ["", ".gz"]:
                tryFile = directory + filename + suffix
                if os.path.isfile(tryFile):
                    data = tryFile
                    break
        if data is None:
            raise Exception("Cannot find file " + filename)
        self.process_file(rel, data, separator,
                          lambda tpl: self.files.outputData( \
                              "insert " + ri.name + "(" + ",".join(tpl) + ")", ","))

    def process_output(self, outputdecl):
        directives = getField(outputdecl, "IodirectiveList")
        body = getField(directives, "IodirectiveBody")
        rel = self.get_relid(body)

        ri = Relation.get(rel, self.current_component)
        ri.isoutput = True
        if skip_files or self.preprocess:
            Relation.dumpOrder.append(ri.name)
            return

        filename = rel
        print "Reading output", rel, "from", filename
        data = None
        for suffix in ["", ".csv"]:
            tryFile = filename + suffix
            if os.path.isfile(tryFile):
                data = tryFile
                break
        if data is None:
            print "*** Cannot find output file " + filename + "; the reference output will be incomplete"
            return
        self.files.dumpFile.write(rel + ":\n")
        self.process_file(rel, data, "\t",
                          lambda tpl: self.files.dumpFile.write( \
                              ri.name + "{" + ",".join(tpl) + "}\n"))

    def process_component(self, component):
        ident = getIdentifier(component)
        if self.current_component != None:
            raise Exception("Nested components: " + self.current_component + "." + ident)
        self.current_component = ident
        self.process(component)
        self.current_component = None

    @staticmethod
    def arg_is_varname(arg):
        """If this arg represents just an identifier, which should be a
        variable name, then it returns the variable name.  Else it returns None"""
        ident = getOptField(arg, "Identifier")
        if ident is None:
            return None
        at = getOptField(arg, "@")
        if at is not None:
            return None
        asopt = getOptField(arg, "as")
        if asopt is not None:
            return None

        return var_name(ident.value)

    def convert_op(self, binop):
        op = binop.value
        if op in self.opdict:
            return self.opdict[op]
        return op

    def convert_arg(self, arg):
        # print arg.tree_str()
        string = getOptField(arg, "String")
        if string is not None:
            return "string_intern(" + string.value + ")"

        us = getOptField(arg, "_")
        if us is not None:
            return "_"

        dlr = getOptField(arg, "$")
        if dlr is not None:
            return "random()"

        at = getOptField(arg, "@")
        if at is not None:
            raise Exception("Functor call not handled")

        args = getArray(arg, "Arg")
        asv = getOptField(arg, "AS")
        if asv is not None:
            raise Exception("AS not handled")

        bk = getOptField(arg, "[")
        if bk is not None:
            raise Exception("[ not handled")

        args = getArray(arg, "Arg")
        if len(args) == 1:
            paren = getOptField(arg, "(")
            if paren is not None:
                rec = self.convert_arg(args[0])
                return "(" + rec + ")"

            # Unary operator
            rec = self.convert_arg(args[0])
            op = self.convert_op(arg.children[0])
            if op == "-":
                return "( 0 " + op + " " + rec + ")"
            return op + " " + rec

        ident = getOptField(arg, "Identifier")
        if ident != None:
            v = var_name(ident.value)
            self.all_variables.add(v)
            if self.converting_head and v != "_":
                self.lhs_variables.add(v)
            if self.converting_tail and v != "_":
                self.bound_variables.add(v)
            return v

        num = getOptField(arg, "NUMBER")
        if num != None:
            # TODO: convert various literals
            return num.value

        if len(args) == 2:
            # Binary operator
            l = self.convert_arg(args[0])
            r = self.convert_arg(args[1])
            binop = arg.children[1]
            op = self.convert_op(binop)
            if op == "^":
                return "exp(" + l + ", " + r + ")"
            return l + " " + op + " " + r

        func = getOptField(arg, "FunctionCall")
        if func != None:
            return self.convert_function_call(func)

        agg = getOptField(arg, "Aggregate")
        if agg != None:
            return self.convert_aggregate(agg)

        raise Exception("Unexpected argument " + arg.tree_str())

    def convert_aggregate(self, agg):
        """Converts an aggregate call.  Sets 'self.aggregate_prefix' to a set of
        expressions that are evaluated before the aggregation"""

        # Must process the bound variables before the expression
        result = "Aggregate(("
        result += ", ".join(self.bound_variables)
        result += "), "

        arg = getOptField(agg, "Arg")
        call = ""
        if arg is not None:
            call = self.convert_arg(arg)
        body = getField(agg, "AggregateBody")
        atom = getOptField(body, "Atom")
        assert self.aggregate_prefix == "", "Nested aggregate calls: " + self.aggregate_prefix
        if atom is not None:
            self.aggregate_prefix = self.convert_atom(atom)
        elif body is not None:
            terms = getListField(body, "Term", "Conjunction")
            terms = [self.convert_term(t) for t in terms]
            self.aggregate_prefix = ", ".join(terms)
        else:
            raise Exception("Unhandled aggregate " + agg.tree_str())

        self.aggregate_prefix += ", "
        func = agg.children[0].value
        if func == "count":
            result += "count(()))"
        else:
            result += "group_" + func + "(" + call + "))"
        return result

    def convert_atom(self, atom):
        name = self.get_relid(atom)
        args = getListField(atom, "Arg", "ArgList")
        args_strings = [self.convert_arg(arg) for arg in args]
        rel = Relation.get(name, self.current_component)
        return rel.name + "(" + ", ".join(args_strings) + ")"

    @staticmethod
    def get_function_name(fn):
        return fn.children[0].value

    def convert_function_call(self, function):
        # print function.tree_str()
        func = self.get_function_name(function)
        args = getListField(function, "Arg", "FunctionArgumentList")
        argStrings = [self.convert_arg(arg) for arg in args]
        if func == "match":
            # Match is reserved keyword in DDlog
            func = "re_match"
        return func + "(" + ", ".join(argStrings) + ")"

    def convert_literal(self, lit):
        t = getOptField(lit, "TRUE")
        if t is not None:
            return "true"

        f = getOptField(lit, "FALSE")
        if f != None:
            return "false"

        relop = getOptField(lit, "Relop")
        if relop is not None:
            args = getArray(lit, "Arg")
            assert len(args) == 2
            op = relop.children[0].value
            prefix = ""
            # This could be translated as equality test or assignment,
            # depending on whether the lhs variable is bound
            varname = None
            if op == "=":
                varname = self.arg_is_varname(args[0])
                if varname is not None:
                    if varname in self.bound_variables:
                        op = "=="
                    else:
                        op = "="
                        prefix = "var "
            l = self.convert_arg(args[0])
            if varname is not None and op == "=":
                self.bound_variables.remove(varname)
            # This call may convert an aggregate, setting aggregate_prefix in the process
            r = self.convert_arg(args[1])
            if self.aggregate_prefix != "":
                assert varname is not None, "Could not find variable assigned by aggregate computation"
                fresh = self.fresh_variable("tmp")
                # This works because all aggregate functions return numeric types where we can apply bit slices
                # For aggregates over strings this is not correct.
                result = self.aggregate_prefix + "var " + fresh + " = " + r + ", var " + l + " = " + fresh + "[31:0]"
                self.aggregate_prefix = ""
            else:
                result = prefix + l + " " + op + " " + r
            return result

        atom = getOptField(lit, "Atom")
        if atom is not None:
            return self.convert_atom(atom)

        func = getOptField(lit, "FunctionCall")
        if func is not None:
            return self.convert_function_call(func)

        raise Exception("Unexpected literal" + lit.tree_str())

    @staticmethod
    def literal_has_relation(literal):
        atom = getOptField(literal, "Atom")
        if atom is None:
            return False
        return True

    def term_has_relation(self, term):
        lit = getOptField(term, "Literal")
        if lit is not None:
            return self.literal_has_relation(lit)
        term = getOptField(term, "Term")
        if term is not None:
            return self.term_has_relation(term)

    def convert_term(self, term):
        # print term.tree_str()
        lit = getOptField(term, "Literal")
        if lit is not None:
            return self.convert_literal(lit)
        term = getOptField(term, "Term")
        if term is not None:
            return "not " + self.convert_term(term)
        raise Exception("Unpexpected term " + term.tree_str())

    def terms_have_relations(self, terms):
        """True if a conjunction contains any relations"""
        flat = reduce(lambda a, b: a + b, terms, [])
        flags = [self.term_has_relation(x) for x in flat]
        return reduce(lambda a, b: a or b, flags, False)

    def convert_terms(self, terms):
        return [self.convert_term(t) for t in terms]

    def process_rule(self, rule):
        """Convert a rule and emit the output"""
        # print rule.tree_str()
        head = getField(rule, "Head")
        tail = getField(rule, "Body")
        self.lhs_variables.clear()
        headAtoms = getList(head, "Atom", "Head")
        disjunctions = getList(tail, "Conjunction", "Body")
        terms = [getList(l, "Term", "Conjunction") for l in disjunctions]

        if not self.terms_have_relations(terms) and self.preprocess:
            # If there are no clauses in the tail we
            # mark all input relations as input relations
            for atom in headAtoms:
                name = self.get_relid(atom)
                ri = Relation.get(name, self.current_component)
                ri.isInput = True
        else:
            self.converting_head = True
            heads = [self.convert_atom(x) for x in headAtoms]
            self.converting_head = False
            self.bound_variables.clear()
            self.converting_tail = True
            convertedDisjunctions = [self.convert_terms(x) for x in terms]
            if len(convertedDisjunctions) > 0 and \
               len(convertedDisjunctions[0]) > 0 and \
               convertedDisjunctions[0][0].startswith("not"):
                # DDlog does not support rules that start with a negation.
                # Insert a reference to a dummy non-empty relation before.
                convertedDisjunctions[0].insert(0, self.dummyRelation + "(0)")
            self.converting_tail = False
            for d in convertedDisjunctions:
                self.files.output(",\n\t".join(heads) + " :- " + ", ".join(d) + ".")

    def process_relation_decl(self, relationdecl):
        """Process a relation declaration and emit output to files"""
        idents = getListField(relationdecl, "Identifier", "RelationList")
        body = getField(relationdecl, "RelationBody")
        params = getListField(body, "Parameter", "ParameterList")
        q = getListField(relationdecl, "Qualifier", "Qualifiers")
        qualifiers = [qual.children[0].value for qual in q]
        if self.preprocess:
            for ident in idents:
                # print "Decl", ident.value, qualifiers
                rel = Relation.create(ident.value, self.current_component)
                if "output" in qualifiers:
                    rel.isoutput = True
                if "input" in qualifiers:
                    rel.isinput = True
                for param in params:
                    arr = getArray(param, "Identifier")
                    rel.addParameter(arr[0].value, arr[1].value)
            return

        for ident in idents:
            rel = Relation.get(ident.value, self.current_component)
            self.files.output(rel.declaration())

    def process_fact_decl(self, fact):
        """Process a fact"""
        if self.preprocess:
            return
        atom = getField(fact, "Atom")
        head = self.convert_atom(atom)
        self.files.output(head + ".")

    def convert_typeid(self, typeid):
        l = getList(typeid, "Identifier", "TypeId")
        strgs = [s.value for s in l]
        return ".".join(strgs)

    def process_type(self, typedecl):
        ident = getIdentifier(typedecl)
        if self.preprocess:
            l = getOptField(typedecl, "UnionType")
            numtype = getOptField(typedecl, "NUMBER_TYPE")
            equiv = None
            if l is not None:
                # If we have a union type we expect all members
                # of the list to be really equivalent types, so we only
                # get the first one.
                tid = getField(l, "TypeId")
                equiv = self.convert_typeid(tid)
            elif numtype is not None:
                equiv = "number"
            typ = Type.create(ident, equiv)
            return typ
        typ = Type.get(ident)
        self.files.output(typ.declaration())
        return typ

    def process_decl(self, decl):
        """Process a declaration; dispatches on declaration kind"""
        typedecl = getOptField(decl, "TypeDecl")
        if typedecl != None:
            self.process_type(typedecl)
            return
        fact_decl = getOptField(decl, "Fact")
        if fact_decl != None:
            self.process_fact_decl(fact_decl)
            return
        relationdecl = getOptField(decl, "RelationDecl")
        if relationdecl != None:
            self.process_relation_decl(relationdecl)
            return
        inputdecl = getOptField(decl, "InputDecl")
        if inputdecl != None:
            self.process_input(inputdecl)
            return
        outputdecl = getOptField(decl, "OutputDecl")
        if outputdecl != None:
            self.process_output(outputdecl)
            return
        rule = getOptField(decl, "Rule")
        if rule != None:
            if self.preprocess:
                return
            self.process_rule(rule)
            return
        component = getOptField(decl, "Component")
        if component != None:
            self.process_component(component)
            return
        init = getOptField(decl, "Init")
        if init != None:
            ids = getArray(init, "Identifier")
            self.path_rename[ids[0].value] = ids[1].value
            return
        raise Exception("Unexpected node " + decl.tree_str())

    def process(self, tree):
        if not self.preprocess:
            # Create a dummy relation with one variable
            self.dummyRelation = self.fresh_variable("Rdummy")
            self.files.output("relation " + self.dummyRelation + "(x: Tnumber)")
            self.files.output(self.dummyRelation + "(0).")
        decls = getListField(tree, "Declaration", "DeclarationList")
        for decl in decls:
            self.process_decl(decl)

def main():
    argParser = argparse.ArgumentParser("souffle-converter.py",
                                        description="Converts programs from Souffle Datalog into DDlog")
    argParser.add_argument("-p", "--prefix", help="Prefix to add to relations written in .dat files")
    argParser.add_argument("input", help="input Souffle program", type=str)
    argParser.add_argument("out", help="Output file prefix data file", type=str)
    argParser.add_argument("-d", help="Debug parser", action="store_true")
    args = argParser.parse_args()
    inputName, outputPrefix = args.input, args.out
    global relationPrefix
    relationPrefix = args.prefix if args.prefix else ""
    files = Files(inputName, outputPrefix)
    parser = getParser(args.d)
    tree = parser.parse_file(files.inputName)
    converter = SouffleConverter(files)
    converter.preprocess = True
    converter.process(tree)
    converter.preprocess = False
    converter.process(tree)
    files.done(Relation.dumpOrder)

if __name__ == "__main__":
    main()
