ACM=acm
TECH=techreport
TEXPARTS= *.tex
BIBPARTS= *.bib
FIGPARTS= figs

TEXFILE = abstract.tex \
					intro.tex \
					design.tex \
					eval.tex\
					full.tex\
					relatedwork.tex\
					tablefs.tex\
					giga.tex\
					summary.tex

$(ACM).pdf: $(ACM).tex $(TEXFILE) reference.bib
	pdflatex $(ACM).tex
	bibtex $(ACM)
	pdflatex $(ACM).tex
	pdflatex $(ACM).tex

$(TECH).pdf: $(TECH).tex $(TEXFILE) reference.bib
	pdflatex $(TECH).tex
	bibtex $(TECH)
	pdflatex $(TECH).tex
	pdflatex $(TECH).tex

paper: acm.pdf
	open acm.pdf &

techreport: techreport.pdf
	open techreport.pdf &

clean:
	\rm -f *.dvi *.bbl *.aux *.log *.blg *.toc *.synctex.gz *.pdf

spotless: clean
	\rm -f $(MAIN).pdf

all.tar:
	tar cvfh all.tar makefile $(MAIN).tex $(TEXPARTS) $(BIBPARTS) \
	$(FIGPARTS) README psfig.sty

