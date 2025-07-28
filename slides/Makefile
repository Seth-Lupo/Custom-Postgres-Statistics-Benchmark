# Makefile for LaTeX Beamer presentation

MAIN = presentation
LATEX = pdflatex
LATEXFLAGS = -interaction=nonstopmode -halt-on-error

.PHONY: all clean view

all: $(MAIN).pdf

$(MAIN).pdf: $(MAIN).tex
	$(LATEX) $(LATEXFLAGS) $(MAIN).tex
	$(LATEX) $(LATEXFLAGS) $(MAIN).tex

clean:
	rm -f $(MAIN).aux $(MAIN).log $(MAIN).nav $(MAIN).out $(MAIN).snm $(MAIN).toc $(MAIN).vrb $(MAIN).pdf

view: $(MAIN).pdf
	open $(MAIN).pdf

help:
	@echo "Available targets:"
	@echo "  make        - Compile the presentation"
	@echo "  make clean  - Remove auxiliary files"
	@echo "  make view   - Compile and open the PDF"
	@echo "  make help   - Show this help message"