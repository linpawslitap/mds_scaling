ACM=acm
TEXPARTS= *.tex
BIBPARTS= *.bib
FIGPARTS= figs

$(ACM).pdf: $(ACM).tex reference.bib
	pdflatex $(ACM).tex
	bibtex $(ACM)
	pdflatex $(ACM).tex
	pdflatex $(ACM).tex

paper: acm.pdf
	open acm.pdf &

clean:
	\rm -f *.dvi *.bbl *.aux *.log *.blg *.toc *.synctex.gz *.pdf

spotless: clean
	\rm -f $(MAIN).pdf

all.tar:
	tar cvfh all.tar makefile $(MAIN).tex $(TEXPARTS) $(BIBPARTS) \
	$(FIGPARTS) README psfig.sty

