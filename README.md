# duplicity_recovery_tools
Tools & Notes I used to recover from duplicity volume corruption on a full backup


I previously had done a full backup of a filesystem using duplicity and had verified this backup as good/healthy post-backup & post-test-restore with these backup files. Unfortunately in transfer a few volumes became corrupted which caused duplicity to fail in attempting restores. I wasn't able to work-around this with duplicity so I was left to extract what I could of the data myself. This was the only backup of that data which I had so I put together some tooling to help stitch back together what I could of that data. 

## Grabbing checksums to identify impacted volumes
I'd previously captured checksums of all gpg files, doing this will give you a checksum that you can check against the manifest to identify what volumes have been impacted. 

```ls -1 *.gpg | parallel -j 4 sha1sum {} > sha1sum.txt```

## Identification of compromised volumes / Potentially lost data
```
 ./verify_volumes.py 
usage: verify_volumes.py [-h] [--manifest MANIFEST] [--checksum-file CHECKSUM_FILE] [--passphrase PASSPHRASE] [--show-all] [-v] backup_dir
verify_volumes.py: error: the following arguments are required: backup_dir
```
[verify_volumes.py](./verify_volumes.py) can be used along with the manifest to identify what volumes may be corrupted and to identify what files in those volumes could be corrupted as well. 

## Decrypting files 
```seq 1 14442 | while read num; do ls -l $PATH/duplicity-full.20250109T044755Z.vol${num}.difftar.gpg;  echo "PASSWORD1234"|gpg --decrypt --batch --pinentry-mode loopback --passphrase-fd 0 $PATH/duplicity-full.20250109T044755Z.vol${num}.difftar.gpg >  $PATH/duplicity-full.20250109T044755Z.vol${num}.difftar   ; done```


 
## Extracting difftars/tarballs
```seq 1 14442 | while read num; do ls -l duplicity-full.20250109T044755Z.vol${num}.difftar; tar -xf duplicity-full.20250109T044755Z.vol${num}.difftar > duplicity-full.20250109T044755Z.vol${num}.difftar.log 2>&1 ; done```

## Reassembling files

Tool [reaassemble_multivol.py](./reassemble_multivol.py) will take a directory structure which already has had the files decrypted & extracted from tar and re-assemble those fragements from the multivol snapshot directory and deposit them into the snapshot direcctory. If you're recovering from a full backup/restore here that snapshot directory, when done will be the closest reflection of the filesystem you backed up.   

```bash
./reassemble_multivol.py multivol_snapshot snapshot  --cleanup -v
[INFO] Multi-volume directory: ./multivol_snapshot
[INFO] Output directory: ./snapshot
[INFO] Workers: 3
[INFO] Chunk size: 8,388,608 bytes
[INFO] Cleanup after assembly: True
[INFO] Dry run: False
[INFO] Scanning for leaf directories...
[INFO] Found 556253 leaf directories to reassemble
[INFO] Starting reassembly with 3 workers...
[INFO] Progress: 10/556253 files reassembled (0.0%)
[INFO] Progress: 20/556253 files reassembled (0.0%)
[INFO] Progress: 30/556253 files reassembled (0.0%)
[INFO] Progress: 40/556253 files reassembled (0.0%)
[INFO] Progress: 50/556253 files reassembled (0.0%)
[INFO] Progress: 60/556253 files reassembled (0.0%)
[INFO] Progress: 70/556253 files reassembled (0.0%)
[INFO] Progress: 80/556253 files reassembled (0.0%)
[INFO] Progress: 90/556253 files reassembled (0.0%)
[INFO] Progress: 100/556253 files reassembled (0.0%)
[INFO] Progress: 110/556253 files reassembled (0.0%)
[INFO] Progress: 120/556253 files reassembled (0.0%)
[INFO] Progress: 130/556253 files reassembled (0.0%)
[INFO] Progress: 140/556253 files reassembled (0.0%)
[INFO] Progress: 150/556253 files reassembled (0.0%)
[INFO] Progress: 160/556253 files reassembled (0.0%)
[INFO] Progress: 170/556253 files reassembled (0.0%)
[INFO] Progress: 180/556253 files reassembled (0.0%)
.
.
.
[INFO] Progress: 556253/556253 files reassembled (100.0%)
[DEBUG] ✓ checksums.txt: Assembled 3360 fragments → 220,149,691 bytes
[INFO] ============================================================
[INFO] Reassembly complete!
[INFO]   Success: 556253
[INFO]   Errors:  0
[INFO]   Total:   556253
[INFO] ============================================================



```

Depending on your compute resources the above processes may take days. 
