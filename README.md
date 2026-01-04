# duplicity_recovery_tools
Tools & Notes I used to recover from duplicity volume corruption on a full backup


I previously had done a full backup of a filesystem using duplicity and had verified this backup as good/healthy post-backup & post-test-restore with these backup files. Unfortunately in transfer a few volumes became corrupted which caused duplicity to fail in attempting restores. I wasn't able to work-around this with duplicity so I was left to extract what I could of the data myself. This was the only backup of that data which I had so I put together some tooling to help stitch back together what I could of that data. 

## Grabbing checksums to identify impacted volumes
I'd previously captured checksums of all gpg files, doing this will give you a checksum that you can check against the manifest to identify what volumes have been impacted. 

> ls -1 *.gpg | parallel -j 4 sha1sum {} > sha1sum.txt

## Decrypting files 
> cat xac |while read file; do newfile=$(echo $file|sed s/\.gpg$//); ls -l /media/brian/recovery/disk3/data/$file;  echo "PASSWORD1234"|gpg --decrypt --batch --pinentry-mode loopback --passphrase-fd 0 $file > $newfile ; done

Depending on what chunk size you'd configured, this will leave you with thousands to millions of small files that need to be re-assembled. For my restore I had about 2.8T of archives which was several dozen million small files. 

## Extracting difftars/tarballs
> seq 1 14424 | while read num; do ls -l duplicity-full.20250109T044755Z.vol${num}.difftar; tar -xf duplicity-full.20250109T044755Z.vol${num}.difftar > duplicity-full.20250109T044755Z.vol${num}.difftar.log 2>&1 ; done

## Reassembling files
