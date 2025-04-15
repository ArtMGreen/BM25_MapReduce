#!/usr/bin/env python
import sys

current_document_id = None
current_count = 0


for line in sys.stdin:
    line = line.strip()
    document_id, title, count = line.split('\t')
    try:
        count = int(count)
    except ValueError:
        continue

    if current_document_id == document_id:
        current_count += count
    else:
        if document_id:
            print(f'{document_id}\t{title}\t{current_count}')
        current_count = count
        current_document_id = document_id

if current_document_id == document_id:
    print(f'{document_id}\t{title}\t{current_count}')
