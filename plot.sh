scp -P 2222 target/Dates-1.0-SNAPSHOT.jar scsch:Dates.jar
ssh -p 2222 scsch '~/Days.sh'
ssh -p 2222 scsch 'rm -r DaysValues'
ssh -p 2222 scsch 'hadoop fs -copyToLocal DaysValues'
scp -P 2222 scsch:DaysValues/part-r-00000 ~/part.csv
~/plot.oct
