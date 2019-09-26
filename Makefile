all:
	stack test

TAGS:
	hasktags src/Language/DifferentialDataflow/*.hs adapters/ovsdb app/*.hs
