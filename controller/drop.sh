#!/bin/bash

hbase shell << 'EOF'
disable 'documents'
drop 'documents'
disable 'documents_bucket'
drop 'documents_bucket'
quit
EOF