#!/bin/bash

mkdir -p pydoc
epydoc -o pydoc/ --graph umlclasstree ../src/haizea/
