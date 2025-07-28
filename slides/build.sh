#!/bin/bash

# Build script for Beamer presentation

echo "Building LaTeX Beamer presentation..."

# Check if pdflatex is installed
if ! command -v pdflatex &> /dev/null; then
    echo "Error: pdflatex not found. Please install a LaTeX distribution."
    echo "On macOS, you can install MacTeX: brew install --cask mactex"
    exit 1
fi

# Compile the presentation
pdflatex -interaction=nonstopmode presentation.tex
if [ $? -eq 0 ]; then
    # Run again for references
    pdflatex -interaction=nonstopmode presentation.tex
    echo "✅ Presentation built successfully: presentation.pdf"
    
    # Ask if user wants to open the PDF
    read -p "Open the PDF? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        open presentation.pdf
    fi
else
    echo "❌ Error building presentation. Check presentation.log for details."
    exit 1
fi