title = paper
latex  = pdflatex --shell-escape --synctex=1
papers = $(title).tex

all: pdf

pdf : $(papers)
	$(latex) $(papers)
	bibtex -min-crossrefs=9000 $(title)
	$(latex) $(papers)
	$(latex) $(papers)

clean:
	rm -f $(addprefix $(title)., pdf log bbl out aux blg synctex.gz)
	rm -f $(addprefix $(title)-latexdiff., pdf log bbl out aux blg synctex.gz)
	rm -f *-diff$(TAG)*
