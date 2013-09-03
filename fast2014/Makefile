PAPER=paper
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

$(PAPER).pdf: $(PAPER).tex $(TEXFILE) reference.bib
	pdflatex $(PAPER).tex
	bibtex $(PAPER)
	pdflatex $(PAPER).tex
	pdflatex $(PAPER).tex

$(TECH).pdf: $(TECH).tex $(TEXFILE) reference.bib
	pdflatex $(TECH).tex
	bibtex $(TECH)
	pdflatex $(TECH).tex
	pdflatex $(TECH).tex

paper: paper.pdf
	open paper.pdf &

techreport: techreport.pdf
	open techreport.pdf &

clean:
	\rm -f *.dvi *.bbl *.aux *.log *.blg *.toc *.synctex.gz *.pdf

spotless: clean
	\rm -f $(MAIN).pdf

all.tar:
	tar cvfh all.tar makefile $(MAIN).tex $(TEXPARTS) $(BIBPARTS) \
	$(FIGPARTS) README psfig.sty

