IEEE= ieee
TEXPARTS= *.tex
BIBPARTS= *.bib
FIGPARTS= figs

$(IEEE).pdf: $(IEEE).tex reference.bib
	pdflatex $(IEEE).tex
	bibtex $(IEEE)
	pdflatex $(IEEE).tex
	pdflatex $(IEEE).tex

paper: ieee.pdf
	open ieee.pdf &

clean:
	\rm -f *.dvi *.bbl *.aux *.log *.blg *.toc *.synctex.gz *.pdf

spotless: clean
	\rm -f $(MAIN).pdf

all.tar:
	tar cvfh all.tar makefile $(MAIN).tex $(TEXPARTS) $(BIBPARTS) \
	$(FIGPARTS) README psfig.sty

