ACM=acm
TEXPARTS= *.tex
BIBPARTS= *.bib
FIGPARTS= figs

TEXFILE = abstract.tex \
					intro.tex \
					design.tex \
					eval.tex\
					relatedwork.tex\
					tablefs.tex\
					giga.tex\
					summary.tex

$(ACM).pdf: $(ACM).tex $(TEXFILE) reference.bib
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

