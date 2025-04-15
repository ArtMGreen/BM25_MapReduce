#!/usr/bin/env python
import sys


for line in sys.stdin:
    line = line.strip()
    try:
        document_id, title, text = line.split("\t", maxsplit=2)
        document_id = int(document_id)
        for word in text.split():
            print(f'{word}\t{document_id}\t1')
    except:
        continue
