TARGET=paper

SOURCES=paper.tex
#	0_abstract.tex \
#	1_intro.tex \
#	2_background.tex \
#	5_conclusion.tex 

GRAPHS=$(wildcard expt-*/*.eps)
FIGS=$(patsubst %.fig,%.pdf,$(wildcard fig/*.fig))

all: $(TARGET).ps
pdf: all

$(TARGET).pdf: Makefile  $(SOURCES) $(FIGS)
	pdflatex  $(TARGET).tex
	-bibtex --min-crossrefs=100    $(TARGET)
	pdflatex  $(TARGET).tex

color: $(TARGET).pdf
	pdflatex $(TARGET).tex
	pdftops $(TARGET).pdf

$(TARGET).ps: $(TARGET).pdf
	pdflatex "\newcommand{\wantBW}{} \input{$(TARGET).tex}"

%.pdf : %.fig #Makefile
	fig2dev -L pdf -b 1 $< $@

%.eps : %.dia #Makefile
	dia --nosplash -e $@ $<

%.pdf : %.eps #Makefile
	epstopdf $<

clean:
	rm -f *.aux *.log *.out *.bbl *.blg *~ *.bak $(FIGS) $(TARGET).ps $(TARGET).pdf
