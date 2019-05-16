#! /usr/bin/env python2.7
"""This program converts Datalog programs written in the Souffle dialect to
   Datalog programs written in the Differential Datalog dialect"""

# pylint: disable=invalid-name,missing-docstring,global-variable-not-assigned,line-too-long
#from __future__ import unicode_literals
import json
import gzip
import os
import argparse
import parglare # parser generator

skip_files = False  # If true do not process .input and .output declarations
skip_logic = False  # If true do not produce file .dl
relationPrefix = "" # Prefix to prepend to all relation names when they are written to .dat files
                    # This makes it possible to concatenate multiple .dat files together
                    # This should end in a dot.
parglareParser = None # Cache here the Parglare parser

### Various utilities

def var_name(ident):
    """Convert a Souffle identifier to one suitable for use as a variable name in DDlog"""
    if ident == "_":
        return ident
    if ident.startswith("?"):
        ident = ident[1:]
    return "_" + ident

def strip_quotes(string):
    assert string[0] == "\"", "String is not quoted"
    assert string[len(string) - 1] == "\"", "String is not quoted"
    return string[1:-1].decode('string_escape')

def dict_intersection(dict1, dict2):
    """Return a dictionary which has the keys common to dict1 and dict2 and the values from dict1"""
    keys = dict1.viewkeys() & dict2.viewkeys()
    return {x: dict1[x] for x in keys}

def dict_append(dict1, dict2):
    """Add everything on dict2 on top of dict1"""
    for k, v in dict2.items():
        dict1[k] = v

def dict_subtract(dict1, dict2):
    """Remove from dict1 all the keys from dict2"""
    for k in dict2:
        if k in dict1:
            del dict1[k]

def join_dict(separator, kvseparator, dictionary):
    """Transform a dictionary to string.  separator is used between items,
    and kvseparator between each key-value"""
    lst = [x + kvseparator + dictionary[x] for x in dictionary]
    return separator.join(lst)

#################################
# The next few functions manipulate Parglare Parse Trees
def getOptField(node, field):
    """Returns the first field named 'field' from 'node', or None if it does not exist"""
    return next((x for x in node.children if x.symbol.name == field), None)

def getField(node, field):
    """Returns the first field named 'field' from 'node'"""
    lst = [x for x in node.children if x.symbol.name == field]
    if lst == []:
        raise Exception("No field " + field + " in " + node.tree_str())
    return lst[0]

def getList(node, field, fields):
    """Returns a list produced by a parglare *right-recursive* rule"""
    if len(node.children) == 0:
        return []
    else:
        result = []
        while True:
            f = getOptField(node, field)
            if f is not None:
                result.append(f)
            tail = getOptField(node, fields)
            if tail is None:
                return result
            else:
                node = tail

def getArray(node, field):
    return [x for x in node.children if x.symbol.name == field]

def getListField(node, field, fields):
    """Given a Parse tree node this gets all the children named field form the child list fields"""
    lst = getField(node, fields)
    return getList(lst, field, fields)

def getParser(debugParser):
    """Parse the Parglare Souffle grammar and get the parser"""
    global parglareParser
    if parglareParser is None:
        print "Creating parser"
        fileName = "souffle-grammar.pg"
        directory = os.path.dirname(os.path.abspath(__file__))
        g = parglare.Grammar.from_file(directory + "/" + fileName)
        parglareParser = parglare.Parser(g, build_tree=True, debug=debugParser)
        print "Parser constructed"
    return parglareParser


def getIdentifier(node):
    """Get a field named "Identifier" from a parglare parse tree node"""
    ident = getField(node, "Identifier")
    return ident.value

################################################33

class Type(object):
    """Information about a type"""
    types = dict() # All types in the program

    def __init__(self, name, outputName):
        assert name is not None
        self.name = name
        self.outputName = outputName
        self.equivalentTo = None
        self.components = None
        self.isnumber = None  # unknown

    @classmethod
    def clear(cls):
        """Delete all existing types"""
        cls.types.clear()

    @classmethod
    def create_tuple(cls, name, components):
        """Create a type that represents a tuple, coming from a record in Souffle"""
        typ = Type(name, "T" + name)
        typ.components = components
        cls.types[name] = typ
        return typ

    @classmethod
    def create(cls, name, equivalentTo):
        """Create a type that is equivalent to another one"""
        if name in cls.types:
            print "Warning: redefinition of type " + name + " ignored"
            return None
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
        """get the type with the specified name.  The name is the original Souffle name"""
        result = cls.types[name]
        assert isinstance(result, cls)
        return result

    def declaration(self):
        """Return a string that is a DDlog declaration for this type"""
        if self.components is not None:
            # Convert records to tuples
            names = [c.outputName for c in self.components]
            return "typedef " + self.outputName + " = " + "Option<(" + ", ".join(names) + ")>"
        e = Type.get(self.equivalentTo)
        return "typedef " + self.outputName + " = " + e.outputName

    def isNumber(self):
        """True if this type is equivalent to number."""
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
        """Return a string that is a DDlogo declaration of this parameter."""
        typ = self.getType()
        return var_name(self.name) + ":" + typ.outputName

    def getType(self):
        """Get the parameter's type"""
        return Type.get(self.typeName)

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
        self.shadow = None

    @classmethod
    def clear(cls):
        """Relete all relations"""
        cls.relations.clear()
        cls.dumpOrder = []

    def addParameter(self, name, typ):
        """Add a parameter to the specified relation, with the given name and type."""
        param = Parameter(name, typ)
        self.parameters.append(param)

    @classmethod
    def get(cls, name, component):
        """Get the relation with the specified name"""
        tryName = SouffleConverter.relative_name(component, name)
        if tryName in cls.relations:
            return cls.relations.get(tryName)
        tryName = name
        if tryName in cls.relations:
            return cls.relations.get(tryName)
        raise Exception("Relation " + name + " not found")

    @classmethod
    def create(cls, name, component):
        assert isinstance(name, basestring)
        name = SouffleConverter.relative_name(component, name)
        if name in cls.relations:
            raise Exception("duplicate relation name " + name)
        ri = Relation(name)
        cls.relations[name] = ri
        # print "Registered relation " + name
        return ri

    def mark_as_input(self):
        # For an input relation generate another shadow relation.
        # This is required since Souffle can modify input relations,
        # while DDlog cannot.  The shadow relation will be the
        # read-only one.
        if self.shadow is not None:
            return
        rel = Relation.create(self.name[1:] + "_shadow", "")
        rel.isinput = True
        self.shadow = rel
        self.shadow.parameters = self.parameters

    def declaration(self):
        result = ""
        if self.isinput:
            result = "input "
        if self.isoutput:
            result = "output "
        result += "relation " + self.name + "(" + ", ".join([a.declaration()
                                                             for a in self.parameters]) + ")"
        if self.shadow is not None:
            result += "\n" + self.shadow.declaration()
            result += "\n" + self.name + "(" + ", ".join([var_name(a.name)
                                                          for a in self.parameters]) + ") :- " + \
                      self.shadow.name + "(" + ", ".join([var_name(a.name)
                                                          for a in self.parameters]) + ")."
        return result

class Component(object):
    """Represents a souffle component."""

    def __init__(self, name, declTree, typeParameters):
        # print "Created component", name, typeParameters
        assert isinstance(typeParameters, list)
        self.name = name
        self.declarations = declTree
        self.inherits = []
        self.overrides = set()   # List of relation names that this component overrides
        self.parameters = typeParameters

    def add_inherits(self, name, typeArgs):
        """Adds a component that this one inherits from"""
        self.inherits.append((name, typeArgs))

    def add_declaration(self, decl):
        self.declarations.append(decl)

    def instantiate(self, typeArgs, converter):
        """Instantiate this component with the specified newName and type arguments"""
        # print "Instantiating", self.name, "as", converter.current_component, \
        #    "with parameters", typeArgs
        assert isinstance(converter, SouffleConverter)
        if typeArgs is not None and len(typeArgs) != len(self.parameters):
            raise Exception("Cannot instantiate " + self.name + " with " +
                                    str(len(typeArgs)) + " it expects " +
                                    str(len(self.parameters)) + " type parameters.")

        # process all ancestor components
        for parentName, typeParams in self.inherits:
            parent = converter.getComponent(parentName)
            assert parent is not None
            # print "Adding overrides of", parent.name, parent.overrides
            save = converter.overridden
            converter.overridden = converter.overridden.union(parent.overrides)
            parentTypeArgs = []

            saveSubstitution = converter.typeSubstitution
            converter.setTypeSubstitution(self.parameters, typeArgs)
            for param in typeParams:
                typ = converter.resolve_type(param)
                parentTypeArgs.append(typ)
            # print "Instantiating parent component", parentName, "with arguments", parentTypeArgs
            parent.instantiate(parentTypeArgs, converter)

            converter.typeSubstitution = saveSubstitution
            converter.overridden = save
        # insert own body
        saveSubstitution = converter.typeSubstitution
        converter.setTypeSubstitution(self.parameters, typeArgs)
        converter.instantiating.append(self)
        converter.process(self.declarations)
        converter.instantiating.pop()
        converter.typeSubstitution = saveSubstitution

class Files(object):
    """Represents the files that are used for input and output"""

    def __init__(self, inputDl, outputPrefix):
        self.inputName = inputDl
        outputName = outputPrefix + ".dl"
        outputDataName = outputPrefix + ".dat"
        outputDumpName = outputPrefix + ".dump.expected"
        if not skip_logic:
            self.outFile = open(outputName, 'w')
        self.output("import intern")
        self.output("import souffle_lib")
        self.output("import souffle_types")
        Type.create("IString", "IString")
        Type.create("bit<32>", "bit<32>")

        # The following are in souffle_types
        t = Type.create("number", "bit<32>")
        t = Type.create("symbol", "IString")
        Type.create("empty", "()")
        if not skip_files:
            self.outputDataFile = open(outputDataName, 'w')
            self.dumpFile = open(outputDumpName, 'w')
            self.outputData("start", ";")
        print "Reading from", self.inputName, "writing output to", \
            outputName, "writing data to", outputDataName

    def output(self, text):
        if text == "":
            return
        if not skip_logic:
            self.outFile.write(text + "\n")
        self.outFile.write(text + "\n")

    def outputData(self, text, terminator):
        self.outputDataFile.write(text + terminator + "\n")

    def done(self, dumporder):
        global relationPrefix
        if not skip_files:
            self.outputData("commit", ";")
            if len(dumporder) == 0:
                self.outputData("dump", ";")
            else:
                for r in dumporder:
                    self.outputData("dump " + relationPrefix + r, ";")
            self.outputData("exit", ";")
            self.outputDataFile.close()
            self.dumpFile.close()
        if not skip_logic:
            self.outFile.close()

##############################################################

def compare(list1, list2, isString):
    """Custom lexicographic comparator for 2 lists.  Each element is compared
    either as a string or as number."""
    assert len(list1) == len(list2) and len(list1) == len(isString), "Lists with different length"
    for i in range(0, len(list1)):
        e1 = list1[i]
        e2 = list2[i]
        if not isString[i]:
            e1 = int(e1)
            e2 = int(e2)
        if e1 < e2:
            return -1
        elif e1 > e2:
            return 1
    return 0

class SouffleConverter(object):
    """Translates a Souffle Datalog program into a DDlog datalog program.
       This class is probably used only as a singleton pattern."""
    # This class is very closely tied to the souffle-grammar.pg grammar.

    def __init__(self, files):
        assert isinstance(files, Files)
        self.files = files
        # During preprocessing we traverse the entire AST and construct various objects.
        # During postprocessing we traverse the AST again and we emit the output to files.
        self.preprocessing = False
        self.current_component = ""  # instance name of the component currently being instantiated
        self.bound_variables = dict() # variables that have been bound, each with an inferred type
        self.all_variables = set()   # all variable names that appear in the program
        self.converting_head = False # True if we are converting a head clause
        self.converting_tail = False # True if we are converting a tail clause
        self.aggregates = ""         # Relations created by evaluating aggregates
        self.dummyRelation = None    # Relation used to convert clauses starting with negative term
        self.opdict = {              # Translation of some Souffle operators
            "band": "&",
            "bor": "|",
            "bxor": "^",
            "bnot": "~",
        }
        self.currentType = None      # Used to guess types for variables
        self.components = {}         # Indexed by name
        self.overridden = set()      # set of relation names that are overridden and should not
        #                              be emitted
        self.instantiating = []      # stack of Component objects that are being instantiated
        self.typeSubstitution = dict() # maps old type name to new type name

    @staticmethod
    def relative_name(component, name, separator="_"):
        """Name of a Souffle object within a component"""
        assert component is not None
        if component == "":
            return name
        return component + separator + name

    def setTypeSubstitution(self, parameters, values):
        """The specified parameters should be substituted with the specified values"""
        self.typeSubstitution.clear()
        if len(parameters) == 0:
            assert values is None or len(values) == 0
            return
        assert values is not None
        assert len(parameters) == len(values)
        for i in range(0, len(parameters)):
            assert parameters[i] != values[i]
            self.typeSubstitution[parameters[i]] = values[i]

    def fresh_variable(self, prefix):
        """Return a variable whose name does not yet occur in the program"""
        name = prefix
        suffix = 0
        while name in self.all_variables:
            name = prefix + "_" + str(suffix)
            suffix = suffix + 1
        self.all_variables.add(name)
        return name

    def getComponent(self, name):
        return self.components[name]

    def getCurrentComponentLegalName(self):
        return self.current_component.replace(".", "_")

    def process_file(self, rel, inFileName, inSeparator, sort):
        """Process an INPUT or OUTPUT with name inFileName.
        rel: is the relation name that is being processed
        inFileName: is the file which contains the data
        inSeparator: is the input record separator
        sort: is a Boolean indicating whether the records in the output
        should be lexicographically sorted.
        Returns the data in the file as a list of lists of strings.

        """
        assert not self.preprocessing
        ri = Relation.get(rel, self.getCurrentComponentLegalName())
        params = ri.parameters
        converter = []
        isString = []
        for p in params:
            typ = p.getType()
            assert isinstance(typ, Type)
            if typ.isNumber():
                isString.append(False)
                converter.append(str)
            else:
                isString.append(True)
                converter.append(lambda a: json.dumps(a, ensure_ascii=False))

        if inFileName.endswith(".gz"):
            data = gzip.open(inFileName, "r")
        else:
            data = open(inFileName, "r")
        lineno = 0

        output = []
        for line in data:
            fields = line.rstrip('\n').split(inSeparator)
            result = []
            # Special handling for the empty tuple, which seems
            # to be written as () in Souffle, instead of the emtpy string
            if len(converter) == 0 and len(fields) == 1 and fields[0] == "()":
                fields = []
            elif len(fields) != len(converter):
                raise Exception(inFileName + ":" + str(lineno) +
                                " does not match schema (expected " +
                                str(len(converter)) + " parameters):" + line)
            for i in range(len(fields)):
                result.append(converter[i](fields[i]))
            output.append(result)
            lineno = lineno + 1
        data.close()

        if sort:
            output = sorted(output, cmp=lambda x, y: compare(x, y, isString))
        return output

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

    @staticmethod
    def get_relid(decl):
        # print decl.tree_str()
        lst = getListField(decl, "Identifier", "RelId")
        values = [e.value for e in lst]
        return "_".join(values)

    def generateFilenames(self, rel):
        """
        A list or files that could contain the data in the
        relation
        :param rel: relation name
        """
        if self.current_component == "":
            return [rel]
        return [self.current_component + "." + rel,
                self.current_component.replace(".", "-") + "-" + rel]

    def process_input(self, inputdecl):
        directives = getField(inputdecl, "IodirectiveList")
        body = getField(directives, "IodirectiveBody")
        rel = self.get_relid(body)
        ri = Relation.get(rel, self.getCurrentComponentLegalName())
        ri.mark_as_input()

        if skip_files or self.preprocessing:
            return

        kvpf = getOptField(body, "KeyValuePairs")
        if kvpf is not None:
            kvp = self.get_kvp(kvpf)
        else:
            kvp = dict()

        if ("IO" in kvp) and (kvp["IO"] != "file"):
            raise Exception("Unexpected IO source " + kvp["IO"])

        if "filename" not in kvp:
            filenames = [f + ".facts" for f in self.generateFilenames(rel)]
        else:
            filenames = [kvp["filename"]]

        if "separator" not in kvp:
            separator = '\t'
        else:
            separator = kvp["separator"]

        global relationPrefix
        print "Reading", rel + "_shadow"
        data = None
        for directory in ["./", "./facts/"]:
            for suffix in ["", ".gz"]:
                for filename in filenames:
                    tryFile = directory + filename + suffix
                    if os.path.isfile(tryFile):
                        data = tryFile
                        break
        if data is None:
            print "** Cannot find input file for " + rel
            return
        output = self.process_file(rel, data, separator, False)
        for row in output:
            self.files.outputData(
                "insert " + relationPrefix + ri.name + "_shadow(" + ",".join(row) + ")", ",")

    def process_output(self, outputdecl):
        if getOptField(outputdecl, "OUTPUT") is None:
            return
        directives = getField(outputdecl, "IodirectiveList")
        body = getField(directives, "IodirectiveBody")
        rel = self.get_relid(body)

        ri = Relation.get(rel, self.getCurrentComponentLegalName())
        ri.isoutput = True
        if skip_files or self.preprocessing:
            Relation.dumpOrder.append(ri.name)
            return

        filenames = self.generateFilenames(rel)
        print "Reading output relation", rel
        data = None
        for suffix in ["", ".csv"]:
            for filename in filenames:
                tryFile = filename + suffix
                if os.path.isfile(tryFile):
                    data = tryFile
                    break
        if data is None:
            print "*** Cannot find output file for " + rel + \
                  "; the reference output will be incomplete"
            return
        output = self.process_file(rel, data, "\t", True)
        for row in output:
            self.files.dumpFile.write(
                relationPrefix + ri.name + "{" + ",".join(row) + "}\n")

    def get_type_parameters(self, typeParameters):
        """Returns the type parameters as a list or None if there are no type parameters
           The same grammar production is used for type arguments."""
        # print typeParameters.tree_str()
        tps = getField(typeParameters, "TypeParameters")
        if getOptField(tps, "TypeParameterList") is not None:
            plist = getListField(tps, "TypeId", "TypeParameterList")
            return [self.convert_typeid(t) for t in plist]
        return []

    def process_component(self, component):
        ct = getField(component, "ComponentType")
        ident = getIdentifier(ct)
        body = getField(component, "ComponentBody")
        typeParams = self.get_type_parameters(ct)
        if self.preprocessing:
            if ident in self.components:
                raise Exception("Duplicate component name: " + ident)
            comp = Component(ident, body, typeParams)
            self.components[ident] = comp
            inherits = getOptField(component, "ComponentTypeList")
            if inherits is not None:
                parents = getListField(component, "ComponentType", "ComponentTypeList")
                for parent in parents:
                    name = getIdentifier(parent)
                    typeParams = self.get_type_parameters(parent)
                    comp.add_inherits(name, typeParams)

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

        nil = getOptField(arg, "NIL")
        if nil is not None:
            return "None"

        us = getOptField(arg, "_")
        if us is not None:
            return "_"

        dlr = getOptField(arg, "$")
        if dlr is not None:
            return "random()"

        at = getOptField(arg, "@")
        if at is not None:
            raise Exception("Functor call not handled")

        # args = getArray(arg, "Arg")
        asv = getOptField(arg, "AS")
        if asv is not None:
            raise Exception("AS not handled")

        bk = getOptField(arg, "[")
        if bk is not None:
            fields = getListField(arg, "Arg", "RecordList")
            converted = [self.convert_arg(a) for a in fields]
            return "Some{(" + ", ".join(converted) + ")}"

        args = getArray(arg, "Arg")
        if len(args) == 1:
            paren = getOptField(arg, "(")
            if paren is not None:
                rec = self.convert_arg(args[0])
                return "(" + rec + ")"

            # Unary operator
            rec = self.convert_arg(args[0])
            op = self.convert_op(arg.children[0])
            self.currentType = "Tnumber"
            if op == "-":
                return "(0 - " + rec + ")"
            elif op == "lnot":
                return "lnot(" + rec + ")"
            return "(" + op + " " + rec + ")"

        ident = getOptField(arg, "Identifier")
        if ident is not None:
            v = var_name(ident.value)
            self.all_variables.add(v)
            if self.converting_tail and v != "_":
                self.bound_variables[v] = self.currentType
            return v

        num = getOptField(arg, "NUMBER")
        if num is not None:
            val = num.value
            if val.startswith("0x"):
                val = "32'h" + val[2:]
            elif val.startswith("0b"):
                val = "32'b" + val[2:]
            return "(" + val + ":bit<32>)"

        if len(args) == 2:
            # Binary operator
            self.currentType = "Tnumber"
            binop = arg.children[1]
            op = self.convert_op(binop)
            left = self.convert_arg(args[0])
            right = self.convert_arg(args[1])
            if op == "^":
                return "pow32(" + left + ", " + right + ")"
            elif (op == "land") or (op == "lor"):
                return op + "(" + left + ", " + right + ")"
            return "(" + left + " " + op + " " + right + ")"

        func = getOptField(arg, "FunctionCall")
        if func is not None:
            return self.convert_function_call(func)

        raise Exception("Unexpected argument " + arg.tree_str())

    @staticmethod
    def arg_contains_aggregate(arg):
        """True if the arg contains an aggregate"""
        agg = getOptField(arg, "Aggregate")
        return agg is not None

    def lit_contains_aggregate(self, literal):
        """True if the literal contains an aggregate"""
        relop = getOptField(literal, "Relop")
        if relop is None:
            return False
        args = getArray(literal, "Arg")
        for arg in args:
            if self.arg_contains_aggregate(arg):
                return True
        return False

    def convert_aggregate(self, variable, right):
        """Convert an aggregate call that assigns to a variable.  This will
           create a temporary relation and insert its declaration in self.aggregates."""
        relname = self.fresh_variable("Ragg")
        agg = getOptField(right, "Aggregate")
        assert agg is not None, "Expected an aggregate" + str(right)
        # Save the bound variables
        save = self.bound_variables
        self.bound_variables.clear()

        body = getField(agg, "AggregateBody")
        atom = getOptField(body, "Atom")
        if atom is not None:
            result = self.convert_atom(atom) + ", "
        elif body is not None:
            terms = getListField(body, "Term", "Conjunction")
            terms = [self.convert_term(t) for t in terms]
            result = ", ".join(terms) + ", "
        else:
            raise Exception("Unhandled aggregate " + agg.tree_str())
        bodyBoundVariables = self.bound_variables

        self.bound_variables.clear()
        arg = getOptField(agg, "Arg")
        call = "()"  # used when there is no argument to aggregate, e.g., count
        if arg is not None:
            call = self.convert_arg(arg)
        argVariables = self.bound_variables

        common = dict_intersection(bodyBoundVariables, save)
        dict_subtract(common, argVariables)

        result += "var " + variable + " = Aggregate(("
        result += ", ".join(common.keys())
        result += "), "

        func = agg.children[0].value
        if func == "count":
            func = "count32"
        result += "group_" + func + "(" + call + "))"

        # Restore the bound variables
        self.bound_variables = save
        args = common
        args[variable] = "Tnumber"
        dict_append(self.bound_variables, args)

        self.aggregates += "relation " + relname + "(" + join_dict(", ", ":", args) + ")\n"
        self.aggregates += relname + "(" + ", ".join(args.keys()) + ") :- " + result + ".\n"
        # Return an invocation of the temporary relation we have created
        return relname + "(" + ", ".join(args.keys()) + ")"

    def convert_atom(self, atom):
        name = self.get_relid(atom)
        rel = Relation.get(name, self.getCurrentComponentLegalName())
        args = getListField(atom, "Arg", "ArgList")
        arg_strings = []
        index = 0
        for arg in args:
            self.currentType = rel.parameters[index].getType().outputName
            index = index + 1
            arg_strings.append(self.convert_arg(arg))
        return rel.name + "(" + ", ".join(arg_strings) + ")"

    @staticmethod
    def get_function_name(fn):
        return fn.children[0].value

    def convert_function_call(self, function):
        """Convert a call to a function"""
        # print function.tree_str()
        func = self.get_function_name(function)
        args = getListField(function, "Arg", "FunctionArgumentList")
        argStrings = [self.convert_arg(arg) for arg in args]
        if func == "match":
            # Match is reserved keyword in DDlog
            func = "re_match"
        return func + "(" + ", ".join(argStrings) + ")"

    def convert_literal(self, lit):
        """Convert a literal from the Souffle grammar"""
        t = getOptField(lit, "TRUE")
        if t is not None:
            return "true"

        f = getOptField(lit, "FALSE")
        if f is not None:
            return "false"

        relop = getOptField(lit, "Relop")
        if relop is not None:
            args = getArray(lit, "Arg")
            assert len(args) == 2
            op = relop.children[0].value
            if op != "=" and op != "!=":
                self.currentType = "Tnumber"
            prefix = "("
            suffix = ")"
            # This could be translated as equality test or assignment,
            # depending on whether the variables that appear are bound or not.
            lvarname = None

            if op == "=":
                # TODO: this does not handle the case of assigning to a tuple
                #  containing some unbound variables
                lvarname = self.arg_is_varname(args[0])
                rvarname = self.arg_is_varname(args[1])
                if (lvarname is not None) and (rvarname is not None) and \
                   (lvarname not in self.bound_variables) and \
                        (rvarname not in self.bound_variables):
                    raise Exception("Comparison between two unbound variables " +
                                    lvarname + op + rvarname)
                if (rvarname is not None) and (rvarname not in self.bound_variables):
                    # swap arguments: this is an assignment to args[1]
                    tmp = args[0]
                    args[0] = args[1]
                    args[1] = tmp
                    lvarname = rvarname
                if (lvarname is not None) and (lvarname not in self.bound_variables):
                    suffix = ""
                    prefix = "var "
                else:
                    op = "=="

            if self.lit_contains_aggregate(lit):
                originalVar = None
                if op == "==":
                    # we have to generate an equality test; we use a temporary variable
                    originalVar = lvarname
                    lvarname = self.fresh_variable("tmp")
                assert lvarname is not None, \
                    "Expected aggregate to assign to variable" + str(args[0])
                result = self.convert_aggregate(lvarname, args[1])
                if originalVar is not None:
                    result += ", " + lvarname + " == " + originalVar
                return result

            # Not an aggregate
            left = self.convert_arg(args[0])
            right = self.convert_arg(args[1])
            result = prefix + left + " " + op + " " + right + suffix
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
        headAtoms = getList(head, "Atom", "Head")
        disjunctions = getList(tail, "Conjunction", "Body")
        terms = [getList(l, "Term", "Conjunction") for l in disjunctions]
        nonOverridden = []
        # print "Overridden:", self.overridden
        for atom in headAtoms:
            rel = self.get_relid(atom)
            if rel in self.overridden:
                # print "Skipping", rel, "overridden"
                pass
            else:
                nonOverridden.append(atom)
        if len(nonOverridden) == 0:
            return

        if not self.terms_have_relations(terms) and self.preprocessing:
            # If there are no clauses in the tail we
            # mark all heads as input relations
            for atom in nonOverridden:
                name = self.get_relid(atom)
                ri = Relation.get(name, self.getCurrentComponentLegalName())
                ri.isInput = True
        else:
            self.converting_head = True
            heads = [self.convert_atom(x) for x in nonOverridden]
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
            self.files.output(self.aggregates)
            self.aggregates = ""

    def resolve_type(self, typeName):
        while typeName in self.typeSubstitution:
            typeName = self.typeSubstitution[typeName]
        return typeName

    def process_relation_decl(self, relationdecl):
        idents = getListField(relationdecl, "Identifier", "RelationList")
        body = getField(relationdecl, "RelationBody")
        params = getListField(body, "Parameter", "ParameterList")
        q = getListField(relationdecl, "Qualifier", "Qualifiers")
        qualifiers = [qual.children[0].value for qual in q]
        if self.preprocessing:
            for ident in idents:
                # print "Decl", ident.value, qualifiers
                rel = Relation.create(ident.value, self.getCurrentComponentLegalName())
                # print "Type substitution", self.typeSubstitution
                for param in params:
                    arr = getArray(param, "Identifier")
                    typeName = arr[1].value
                    typeName = self.resolve_type(typeName)
                    # print "Adding relation parameter", arr[0].value, typeName
                    rel.addParameter(arr[0].value, typeName)

                if "input" in qualifiers:
                    rel.mark_as_input()
                if "output" in qualifiers:
                    rel.isoutput = True
            return

        for ident in idents:
            rel = Relation.get(ident.value, self.getCurrentComponentLegalName())
            self.files.output(rel.declaration())

    def process_fact_decl(self, fact):
        """Process a fact"""
        if self.preprocessing:
            return
        atom = getField(fact, "Atom")
        rel = self.get_relid(atom)
        if rel in self.overridden:
            # print "Skipping", rel, "overridden"
            return
        head = self.convert_atom(atom)
        self.files.output(head + ".")

    @staticmethod
    def convert_typeid(typeid):
        lst = getList(typeid, "Identifier", "TypeId")
        strgs = [s.value for s in lst]
        return ".".join(strgs)

    def process_type(self, typedecl):
        ident = getIdentifier(typedecl)
        if self.preprocessing:
            # print "Creating type", ident
            union = getOptField(typedecl, "UnionType")
            numtype = getOptField(typedecl, "NUMBER_TYPE")
            recordType = getOptField(typedecl, "RecordType")
            sqbrace = getOptField(typedecl, "[")

            if recordType is not None:
                fields = getList(recordType, "TypeId", "RecordType")
                fields = [Type.get(self.convert_typeid(f)) for f in fields]
                Type.create_tuple(ident, fields)
                return
            if sqbrace is not None:
                Type.create(ident, "TEmpty")
                return

            equiv = None
            if union is not None:
                # If we have a union type we expect all members
                # of the list to be really equivalent types, so we only
                # get the first one.
                tid = getField(union, "TypeId")
                equiv = self.convert_typeid(tid)
            elif numtype is not None:
                equiv = "number"
            Type.create(ident, equiv)
            return
        if ident == "number":
            # already declared
            return
        typ = Type.get(ident)
        self.files.output(typ.declaration())

    def process_decl(self, decl):
        """Process a declaration; dispatches on declaration kind"""
        typedecl = getOptField(decl, "TypeDecl")
        if typedecl is not None:
            self.process_type(typedecl)
            return
        fact_decl = getOptField(decl, "Fact")
        if fact_decl is not None:
            self.process_fact_decl(fact_decl)
            return
        relationdecl = getOptField(decl, "RelationDecl")
        if relationdecl is not None:
            self.process_relation_decl(relationdecl)
            return
        inputdecl = getOptField(decl, "InputDecl")
        if inputdecl is not None:
            self.process_input(inputdecl)
            return
        outputdecl = getOptField(decl, "OutputDecl")
        if outputdecl is not None:
            self.process_output(outputdecl)
            return
        rule = getOptField(decl, "Rule")
        if rule is not None:
            if self.preprocessing:
                return
            self.process_rule(rule)
            return
        component = getOptField(decl, "Component")
        if component is not None:
            self.process_component(component)
            return
        init = getOptField(decl, "Init")
        if init is not None:
            ident = getIdentifier(init)
            ctype = getField(init, "CompType")
            compName = getIdentifier(ctype)
            compName = self.resolve_type(compName)
            comp = self.components[compName]
            typeArgs = self.get_type_parameters(ctype)
            saveCurrent = self.current_component
            self.current_component = SouffleConverter.relative_name(
                self.current_component, ident, ".")
            typeArgs = [self.resolve_type(t) for t in typeArgs]
            comp.instantiate(typeArgs, self)
            self.current_component = saveCurrent
            return
        pragma = getOptField(decl, "Pragma")
        if pragma is not None:
            return
        override = getOptField(decl, "Override")
        if override is not None:
            if len(self.instantiating) == 0:
                raise Exception(".override not within a component")
            if not self.preprocessing:
                return
            comp = self.instantiating[-1]
            comp.overrides.add(getIdentifier(override))
            return
        raise Exception("Unexpected node " + decl.tree_str())

    def process_only_facts(self, decl):
        typedecl = getOptField(decl, "TypeDecl")
        if typedecl != None:
            self.process_type(typedecl)
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

    def process(self, tree):
        decls = getListField(tree, "Declaration", "DeclarationList")
        for decl in decls:
            if skip_logic:
                self.process_only_facts(decl)
            else:
                self.process_decl(decl)
        self.files.output(self.aggregates)

def convert(inputName, outputPrefix, relPrefix, debug=False):
    Type.clear()
    Relation.clear()
    global relationPrefix
    relationPrefix = relPrefix if relPrefix else ""
    if relationPrefix != "":
        assert relationPrefix.endswith("."), "prefix is expected to end with a dot"
    files = Files(inputName, outputPrefix)
    parser = getParser(debug)
    tree = parser.parse_file(files.inputName)
    converter = SouffleConverter(files)
    converter.preprocessing = True
    converter.process(tree)

    # Create a dummy relation with one variable.  This is used
    # to handle rules that have only negated terms
    converter.dummyRelation = converter.fresh_variable("Rdummy")
    files.output("relation " + converter.dummyRelation + "(x: Tnumber)")
    files.output(converter.dummyRelation + "(0).")

    # print "=================== finished preprocessing"
    converter.preprocessing = False
    converter.process(tree)
    files.done(Relation.dumpOrder)

def main():
<<<<<<< 56dfa1f0fa8130ac3df294397e348834d59b7870
    argParser = argparse.ArgumentParser("souffle_converter.py",
                                        description="Converts programs from Souffle Datalog into DDlog")
    argParser.add_argument("-p", "--prefix", help="Prefix to add to relations written in .dat files")
=======
    argParser = argparse.ArgumentParser("souffle-converter.py",
                                        description=
                                        "Converts programs from Souffle Datalog into DDlog")
    argParser.add_argument("-p", "--prefix",
                           help="Prefix to add to relations written in .dat files")
>>>>>>> Support almost complete for souffle components
    argParser.add_argument("input", help="input Souffle program", type=str)
    argParser.add_argument("out", help="Output file prefix data file", type=str)
    argParser.add_argument("-d", help="Debug parser", action="store_true")
    argParser.add_argument("--facts-only", "--facts_only", action='store_true', help="produces only facts")
    argParser.add_argument("--logic-only", "--logic_only", action='store_true', help="produces only logic")
    args = argParser.parse_args()
    inputName, outputPrefix = args.input, args.out
    if args.facts_only and args.logic_only:
        raise Exception("Cannot produce only facts and only logic")
    global skip_files, skip_logic
    if args.logic_only:
        skip_files = True
    if args.facts_only:
        skip_logic = True
    convert(args.input, args.out, args.prefix, args.d)

if __name__ == "__main__":
    main()
