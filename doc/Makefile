MAIN= ieee 
TECHREPORT=techreport
IEEE= ieee
TEXPARTS= *.tex
BIBPARTS= *.bib
FIGPARTS= figs

$(MAIN).pdf: $(MAIN).tex reference.bib
	pdflatex $(MAIN).tex
	bibtex $(MAIN)
	pdflatex $(MAIN).tex
	pdflatex $(MAIN).tex

$(TECHREPORT).pdf: $(TECHREPORT).tex reference.bib
	pdflatex $(TECHREPORT).tex
	bibtex $(TECHREPORT)
	pdflatex $(TECHREPORT).tex
	pdflatex $(TECHREPORT).tex

$(IEEE).pdf: $(IEEE).tex reference.bib
	pdflatex $(IEEE).tex
	bibtex $(IEEE)
	pdflatex $(IEEE).tex
	pdflatex $(IEEE).tex

paper: ieee.pdf
	open ieee.pdf &

techreport: techreport.pdf
	open techreport.pdf &

ieee: ieee.pdf
	open ieee.pdf &

clean:
	\rm -f *.dvi *.bbl *.aux *.log *.blg *.toc *.synctex.gz *.pdf

spotless: clean
	\rm -f $(MAIN).pdf

all.tar:
	tar cvfh all.tar makefile $(MAIN).tex $(TEXPARTS) $(BIBPARTS) \
	$(FIGPARTS) README psfig.sty

