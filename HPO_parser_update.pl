use strict;
my $date = `date`;
my $outfolder="/space1/databaseUpdates/";
my @datecols=split(/\s/,$date);
my $datestring="$datecols[1]_$datecols[2]_$datecols[5]";

my $inmap="$datestring.ALL_SOURCES_ALL_FREQUENCIES_diseases_to_genes_to_phenotypes.txt";
my $insql="$datestring.MYHPO.sql";
my $inobo="$datestring.hp.obo";
my $tempsql="$datestring.MYHPO_temp.sql";
my $updatesql="$datestring.MYHPO_update.sql";
unless (-e "$outfolder$inmap"){
	`wget http://compbio.charite.de/hudson/job/hpo.annotations.monthly/lastStableBuild/artifact/annotation/ALL_SOURCES_ALL_FREQUENCIES_diseases_to_genes_to_phenotypes.txt -O $inmap`;
	`mv $inmap $outfolder`;
}
unless (-e "$outfolder$insql"){
	`wget http://compbio.charite.de/hudson/job/hpo.annotations.monthly/lastStableBuild/artifact/annotation/MYHPO_01_2014.sql -O $insql`;
	`mv $insql $outfolder`;
}
unless (-e "$outfolder$inobo"){
	`wget http://compbio.charite.de/hudson/job/hpo/lastStableBuild/artifact/ontology/release/hp.obo -O $inobo`;
	`mv $inobo $outfolder`;
}

print "$outfolder$inmap\n";
print "$outfolder$insql\n";
print "$outfolder$inobo\n";
open(INMAP,"<$outfolder$inmap");
open(SQL,"<$outfolder$insql");
open(INOBO,"<$outfolder$inobo");
open(TEMPSQL,">$outfolder$tempsql");
open(UPDATESQL,">$outfolder$updatesql");

my $md5sum = `md5sum $outfolder$inmap`;
my $file = `ls -lah $outfolder/$inmap`;
chomp($md5sum);
chomp($file);
chomp($date);
print UPDATEOUT "UPDATE METADATA SET fileLocation='$file',insertDate='$date',md5Sum='$md5sum' WHERE tableName='HPO';\n";

# make file for HPO table
<INMAP>;
#print TEMPSQL "CREATE table HPO_temp LIKE HPO;\n";
print TEMPSQL "TRUNCATE HPO_temp;\n";
print TEMPSQL "TRUNCATE term_temp;\n";
print TEMPSQL "DROP TABLE IF EXISTS graph_path_temp;\n";
print TEMPSQL "CREATE table graph_path_temp like graph_path;\n";

my $graph="graph_path";
my $tempgraph="graph_path_temp";

#print UPDATESQL "CREATE table update_HPO as select omimID,hpoID,description,gene from HPO where (omimID,hpoID,description,gene) NOT IN (select HPO.omimID, HPO.hpoID,HPO.description,HPO.gene from HPO,HPO_temp where HPO.omimID=HPO_temp.omimID and HPO.hpoID=HPO_temp.hpoID and HPO.description=HPO_temp.description and HPO.gene=HPO_temp.gene);\n";

#print UPDATESQL "DELETE FROM HPO where (omimID,hpoID,description,gene) in (SELECT omimID,hpoID,description,gene  from update_HPO);\n";
print UPDATESQL "DROP TABLE IF EXISTS update_add_graph_path,update_remove_graph_path;\n";

print UPDATESQL "CREATE table update_remove_graph_path as select term1_id,term2_id,distance from $graph where (term1_id, term2_id, distance) NOT IN (select $graph.term1_id, $graph.term2_id, $graph.distance from $graph,$tempgraph where  $graph.term1_id=$tempgraph.term1_id and $graph.term2_id=$tempgraph.term2_id and $graph.distance=$tempgraph.distance);\n";
print UPDATESQL "CREATE table update_add_graph_path as select term1_id,term2_id,distance from $tempgraph where (term1_id, term2_id, distance) NOT IN (select $tempgraph.term1_id, $tempgraph.term2_id, $tempgraph.distance from $graph,$tempgraph where  $graph.term1_id=$tempgraph.term1_id and $graph.term2_id=$tempgraph.term2_id and $graph.distance=$tempgraph.distance);\n";


print UPDATESQL "DELETE FROM $graph where (term1_id,term2_id,distance) in (SELECT term1_id,term2_id,distance from update_remove_graph_path);\n";


# insert into  HPO_temp table
print TEMPSQL "LOCK TABLES HPO_temp WRITE;\n";
print UPDATESQL "LOCK TABLES HPO WRITE;\n";
while (my $line = <INMAP>){
    chomp($line);
    my @A = split/\t/,$line;
    my $omim = substr($A[0], 5);
    my $hp = substr($A[3], 3);
    my $hpi = int($hp);
    my $desc = $A[4];
    $desc =~s/'/\\'/g;
    my $gene = $A[1];
    print TEMPSQL "INSERT INTO HPO_temp VALUES ($hpi,'$desc','$gene') ON DUPLICATE KEY UPDATE hpoID=$hpi, description='$desc',gene='$gene';\n";
    #print UPDATESQL "INSERT INTO HPO (omimID, hpoID, description, gene) VALUES ('$omim','$hpi','$desc','$gene') ON DUPLICATE KEY UPDATE omimID='$omim', hpoID='$hpi',description='$desc',gene='$gene')\n";
}
print TEMPSQL "UNLOCK TABLES;\n";
print UPDATESQL "UNLOCK TABLES;\n";


# parse hp.obo, find comment defenition, synonym
my %cmthash;
my %defhash;
my %synhash;
my @record;
while (my $line = <INOBO>){
	chomp($line);
	if ($line eq "[Term]"){
		my $id;
		my $comment="NULL";
		my $def="NULL";
		if ($#record > 1){
			foreach my $rec (@record){
				$rec=~ s/\\*//g;
				if ($rec =~ /^id/){
					my @cols=split/:/,$rec;
					$id=$cols[2];
					$id=~ /(0*)(\d*)/;
					$id=$2;	
				}
				if ($rec =~ /^def:/){
					my @cols=split(/"/,$rec);
					$def=$cols[1];
					$def=~ s/\'/\\'/g;
					$defhash{$id}=$def;
				}
				if ($rec =~ /^synonym:/){
					my @cols=split(/"/,$rec);
					push @{$synhash{$id}},$cols[1];
					
				}	
				if ($rec =~ /^comment:/){
					my @cols=split(/:/,$rec);
					$comment=$cols[1];
					$comment =~ s/^\s+|\s+$//g;
					$comment =~ s/\'/\\'/g;
					$cmthash{$id}=$comment;
				}
			}
		}
		@record=();
	}
	push (@record, $line);
}
=pod
foreach my $key(keys (%cmthash)){
	print "$key\t$cmthash{$key}\n";
}
while (my $line = <SQL>){
	 chomp($line);
	 if ($line =~ /^INSERT INTO `term_definition` VALUES*/){
                print TEMPSQL "-- HPO term_definition\n";
		$line =~ s/INSERT INTO `term_definition` VALUES //g;
        	my @cols=split(/\),\(/,$line);
        	foreach my $record (@cols){
                	$record =~ s/[\(|\);]//g;
               	 	chomp($record);
                	$record =~ /(\d+),{1}(.+)/;
			$defhash{$1}=$2;		
        	}
	}
	if ($line =~ /^INSERT INTO `term_synonym` VALUES*/){
                print TEMPSQL "-- HPO term synonym\n";
		 $line =~ s/INSERT INTO `term_synonym_temp` VALUES //g;
        	my @cols=split(/\),\(/,$line);
        	foreach my $record (@cols){
                	$record =~ s/[\(|\);]//g;
                	chomp($record);
                	$record =~ /(\d+),{1}(.+),{1}(\d+)/;
			push @{$synhash{$1}},$2;
        	}	
        }
}
close SQL;
=cut


#make file for graph_path,term
while (my $line = <SQL>){
	chomp($line);
	if ($line =~ /^INSERT INTO `graph_path` VALUES*/){
		print TEMPSQL "LOCK TABLES `graph_path_temp` WRITE;\n";
		$line =~ s/graph_path/graph_path_temp/g;
		print TEMPSQL "$line\n";
		print TEMPSQL "UNLOCK TABLES;\n";
		print UPDATESQL "LOCK TABLES `graph_path` WRITE;\n";
		procgraph_path($line);
		print UPDATESQL "UNLOCK TABLES;\n";
        }
	if ($line =~ /^INSERT INTO `term` VALUES*/){
                print TEMPSQL "-- HPO term table\n";
		print TEMPSQL "LOCK TABLES `term_temp` WRITE;\n";
		#print UPDATESQL "LOCK TABLES `term` WRITE;\n";
		procterm($line);
		print TEMPSQL "UNLOCK TABLES;\n";
		#print UPDATESQL "UNLOCK TABLES;\n";
	}
}
`mysqldump -u testma -ptestma test graph_path > $outfolder$datestring.HPO_graph_path_backup.sql`;


close TEMPSQL;
close UPDATESQL;

sub procgraph_path(){
	my $line=$_[0];
	$line =~ s/INSERT INTO `graph_path_temp` VALUES //g;
        my @cols=split(/\),\(/,$line);
	foreach my $record (@cols){
		$record =~ s/[\(|\);]//g;
		chomp($record);
		my @values=split(/,/,$record);
		print UPDATESQL "INSERT INTO graph_path (term1_id,term2_id,distance) VALUES ($values[0],$values[1],$values[2]) ON DUPLICATE KEY UPDATE term1_id=$values[0],term2_id=$values[1],distance=$values[2];\n";
	}
}

sub procterm(){
	 my $line=$_[0];
        $line =~ s/INSERT INTO `term` VALUES //g;
        my @cols=split(/\),\(/,$line);
        foreach my $record (@cols){
                $record =~ s/[\(|\);]//g;
                chomp($record);
                $record =~ /(\d+),{1}(.+),{1}(\d+),{1}(\d+),{1}(.+),{1}(.+),{1}(.+)/;
		my $synonym;
		my $cmt=$cmthash{$1};
                if (exists $synhash{$1}){
			 foreach (@{$synhash{$1}}){
				my $syn=$_;
				$syn =~ s/\'//g;			
                		$synonym.="$syn, ";
       			 }
			 $synonym =~ s/, $//g;
		}
		my $def;
		if (exists $defhash{$1}){
			$def=$defhash{$1};
			$def =~ s/\'//g;
		}
		print TEMPSQL "INSERT INTO term_temp VALUES ($1,$2,$3,$4,$5,'$cmt',$7,'$def','$synonym');\n";
                #print UPDATESQL "INSERT INTO term (id,name,is_obsolete,is_root,subontology,comment,acc) VALUES ($1,$2,$3,$4,$5,$6,$7) ON DUPLICATE KEY UPDATE id=$1,name=$2,is_obsolete=$3,is_root=$4,subontology=$5,comment=$6,acc=$7;\n";
        }
}





