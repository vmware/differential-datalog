PDFLATEX = pdflatex -interaction=nonstopmode

PAPER = paper

${PAPER}.pdf: *.tex ${PAPER}.bib
	$(PDFLATEX) ${PAPER}
	bibtex ${PAPER}
	$(PDFLATEX) ${PAPER}
	$(PDFLATEX) ${PAPER}

clean:
	rm -f *.dvi
	rm -f ${PAPER}.pdf
	rm -f *.ps
	rm -f *.out
	rm -f *.aux
	rm -f *.blg
	rm -f *.bbl
	rm -f *.log
