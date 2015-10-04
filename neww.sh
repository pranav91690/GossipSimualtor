for i in {1000..2000..1000}
do
  sbt --warn 'set showSuccess := false' "run $i full gossip" >> ./test1.csv
done
