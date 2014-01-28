use strict;
my $date =`date +"%Y-%m-%d %k:%M:%S"`;
my $outfolder="/space1/databaseUpdates/";
my @datecols=split(/-/,$date);
my $datestring="$datecols[1]_$datecols[0]";

my $inmap="$datestring.ALL_SOURCES_ALL_FREQUENCIES_diseases_to_genes_to_phenotypes.txt";
my $insql="$datestring.MYHPO.sql";
my $inobo="$datestring.hp.obo";
my $updatesql="$datestring.MYHPO_update.sql";

#download HPO files (need to change the sql file name)
unless (-e "$outfolder$inmap"){
	`wget http://compbio.charite.de/hudson/job/hpo.annotations.monthly/lastStableBuild/artifact/annotation/ALL_SOURCES_ALL_FREQUENCIES_diseases_to_genes_to_phenotypes.txt -O $inmap`;
	`mv $inmap $outfolder`;
}
unless (-e "$outfolder$insql"){
	`wget http://compbio.charite.de/hudson/job/hpo.annotations.monthly/lastStableBuild/artifact/annotation/MYHPO_$datestring.sql -O $insql`;
	`mv $insql $outfolder`;
}

unless (-e "$outfolder$inobo"){
	`wget http://compbio.charite.de/hudson/job/hpo/lastStableBuild/artifact/ontology/release/hp.obo -O $inobo`;
	`mv $inobo $outfolder`;
}

open(INMAP,"<$outfolder$inmap");
open(SQL,"<$outfolder$insql");
open(INOBO,"<$outfolder$inobo");
open(UPDATESQL,">$outfolder$updatesql");

my $md5sum = `md5sum $outfolder$inmap`;
my $file = `ls -lah $outfolder/$inmap`;
chomp($md5sum);
chomp($file);
chomp($date);
print UPDATESQL "UPDATE METADATA SET fileLocation='$file',insertDate='$date',md5Sum='$md5sum' WHERE tableName='HPO';\n";

# make file for HPO table
<INMAP>;
print UPDATESQL "DROP TABLE IF EXISTS HPO_temp;\n";
print UPDATESQL "DROP TABLE IF EXISTS hpoterm_temp;\n";
print UPDATESQL "DROP TABLE IF EXISTS graph_path_temp;\n";
print UPDATESQL "DROP TABLE IF EXISTS altid;\n";
print UPDATESQL "ALTER TABLE HPOterm add definition varchar(1500);\n";
print UPDATESQL "ALTER TABLE HPOterm add synonyms varchar(1000);\n";
print UPDATESQL "ALTER TABLE HPOgraph_path drop foreign key fk_graph_path_term1;\n";
print UPDATESQL "ALTER TABLE HPOgraph_path drop foreign key fk_graph_path_term2;\n";
print UPDATESQL "ALTER TABLE HPOphenotype drop foreign key HPOphenotype_ibfk_2;\n";

print UPDATESQL "CREATE TABLE hpoterm_temp LIKE HPOterm;\n";
print UPDATESQL "CREATE TABLE HPO_temp LIKE HPO;\n";
print UPDATESQL "CREATE table graph_path_temp like HPOgraph_path;\n";

print UPDATESQL "CREATE TABLE altid (oldid int(11), newid int (11) )ENGINE=InnoDB;\n";

my @update=();
my @temp=();
my $graph="HPOgraph_path";
my $tempgraph="graph_path_temp";

push(@update, "DROP TABLE IF EXISTS HPOterm_add,HPOterm_remove;\n");
push(@update, "CREATE table HPOterm_remove as select id,name,is_obsolete,is_root,subontology,comment,acc,definition,synonyms from HPOterm where ( id,name,is_obsolete,is_root,subontology,comment,acc,definition,synonyms) NOT IN (SELECT  HPOterm.id,HPOterm.name,HPOterm.is_obsolete,HPOterm.is_root,HPOterm.subontology,HPOterm.comment,HPOterm.acc,HPOterm.definition,HPOterm.synonyms from HPOterm,hpoterm_temp where  HPOterm.id=hpoterm_temp.id and HPOterm.name=hpoterm_temp.name and HPOterm.is_obsolete=hpoterm_temp.is_obsolete and HPOterm.is_root=hpoterm_temp.is_root and HPOterm.subontology=hpoterm_temp.subontology and HPOterm.comment=hpoterm_temp.comment and HPOterm.acc=hpoterm_temp.acc and HPOterm.definition=hpoterm_temp.definition and HPOterm.synonyms=hpoterm_temp.synonyms);\n");


push(@update, "CREATE table HPOterm_add as select id,name,is_obsolete,is_root,subontology,comment,acc,definition,synonyms from hpoterm_temp where ( id,name,is_obsolete,is_root,subontology,comment,acc,definition,synonyms) NOT IN (SELECT  hpoterm_temp.id,hpoterm_temp.name,hpoterm_temp.is_obsolete,hpoterm_temp.is_root,hpoterm_temp.subontology,hpoterm_temp.comment,hpoterm_temp.acc,hpoterm_temp.definition,hpoterm_temp.synonyms from HPOterm,hpoterm_temp where  HPOterm.id=hpoterm_temp.id and HPOterm.name=hpoterm_temp.name and HPOterm.is_obsolete=hpoterm_temp.is_obsolete and HPOterm.is_root=hpoterm_temp.is_root and HPOterm.subontology=hpoterm_temp.subontology and HPOterm.comment=hpoterm_temp.comment and HPOterm.acc=hpoterm_temp.acc and HPOterm.definition=hpoterm_temp.definition and HPOterm.synonyms=hpoterm_temp.synonyms);\n");

push(@update, "DELETE FROM HPOterm using HPOterm, HPOterm_remove where HPOterm.id=HPOterm_remove.id and HPOterm.name=HPOterm_remove.name;\n");
push(@update, "INSERT INTO HPOterm (id,name,is_obsolete,is_root,subontology,comment,acc,definition,synonyms) select id,name,is_obsolete,is_root,subontology,comment,acc,definition,synonyms from HPOterm_add;\n");
    	


push(@update, "UPDATE HPO_temp set isCodified=(SELECT DISTINCT(HPO.isCodified) from HPO where HPO.hpoID=HPO_temp.hpoID and HPO.gene=HPO_temp.gene);\n");
push(@update, "DROP TABLE IF EXISTS HPO_add,HPO_remove;\n");
push(@update, "CREATE table HPO_remove as select omimID, hpoID, description, gene,isCodified from HPO where (omimID,hpoID,description,gene,isCodified) NOT IN (SELECT HPO.omimID,HPO.hpoID,HPO.description,HPO.gene,HPO.isCOdified from HPO,HPO_temp where HPO.omimID=HPO_temp.omimID and HPO.hpoID=HPO_temp.hpoID and HPO.description=HPO_temp.description and HPO.gene=HPO_temp.gene and HPO.isCodified=HPO_temp.isCodified);\n");
push(@update, "CREATE table HPO_add as select omimID, hpoID, description, gene,isCodified from HPO_temp where (omimID,hpoID,description,gene,isCodified) NOT IN (SELECT HPO.omimID,HPO.hpoID,HPO.description,HPO.gene,HPO.isCOdified from HPO,HPO_temp where HPO.omimID=HPO_temp.omimID and HPO.hpoID=HPO_temp.hpoID and HPO.description=HPO_temp.description and HPO.gene=HPO_temp.gene and HPO.isCodified=HPO_temp.isCodified);\n");

push (@update, "DELETE FROM HPO using HPO,HPO_remove where HPO.hpoID=HPO_remove.hpoID and HPO.gene=HPO_remove.gene;\n");

push (@update, "INSERT INTO HPO (omimID,hpoID,description,gene,isCodified) SELECT omimID,hpoID, description,gene,isCodified from HPO_add;\n");



push(@update, "DROP TABLE IF EXISTS update_add_graph_path,update_remove_graph_path;\n");

push(@update, "CREATE table update_remove_graph_path as select term1_id,term2_id,distance from $graph where (term1_id, term2_id, distance) NOT IN (select $graph.term1_id, $graph.term2_id, $graph.distance from $graph,$tempgraph where  $graph.term1_id=$tempgraph.term1_id and $graph.term2_id=$tempgraph.term2_id and $graph.distance=$tempgraph.distance);\n");
push(@update, "CREATE table update_add_graph_path as select term1_id,term2_id,distance from $tempgraph where (term1_id, term2_id, distance) NOT IN (select $tempgraph.term1_id, $tempgraph.term2_id, $tempgraph.distance from $graph,$tempgraph where  $graph.term1_id=$tempgraph.term1_id and $graph.term2_id=$tempgraph.term2_id and $graph.distance=$tempgraph.distance);\n");


push(@update, "DELETE FROM $graph using $graph,update_remove_graph_path where $graph.term1_id=update_remove_graph_path.term1_id and $graph.term2_id=update_remove_graph_path.term2_id;\n");


push (@update, "INSERT INTO $graph (term1_id,term2_id,distance) SELECT term1_id,term2_id,distance from update_add_graph_path;\n");

#add back foreign key constraint
push (@update, "ALTER TABLE HPOgraph_path add constraint fk_graph_path_term1 FOREIGN KEY (term1_id) references HPOterm(id);\n");
push (@update, "ALTER TABLE HPOgraph_path add constraint fk_graph_path_term2 FOREIGN KEY (term2_id) references HPOterm(id);\n");
push (@update,  "ALTER TABLE HPOphenotype add constraint HPOphenotype_ibfk_2 FOREIGN KEY (HPOID) references HPOterm(id);\n");

# insert into  HPO_temp table
push(@temp, "LOCK TABLES HPO_temp WRITE;\n");
#push(@update,"LOCK TABLES HPO WRITE;\n");
my $newhpo="INSERT INTO HPO_temp VALUES ";
my @hpoarray=();
while (my $line = <INMAP>){
    chomp($line);
    my @A = split/\t/,$line;
    my $omim = substr($A[0], 5);
    my $hp = substr($A[3], 3);
    my $hpi = int($hp);
    my $desc = $A[4];
    $desc =~s/'/\\'/g;
    my $gene = $A[1];
    push(@hpoarray,"('NULL',$hpi,'$desc','$gene',0),");
}
@hpoarray=uniq(@hpoarray);
foreach my $hpo (@hpoarray){
	$newhpo.=$hpo;
}
$newhpo =~ s/,$/;\n/;
push(@temp, $newhpo);
push(@temp,"UNLOCK TABLES;\n");
#push(@update,"UNLOCK TABLES;\n");


# parse hp.obo, find comment defenition, synonym
my %cmthash;
my %defhash;
my %synhash;
my %altidhash;
my @record;
while (my $line = <INOBO>){
	chomp($line);
	if ($line eq "[Term]"){
		my $id;
		my $altid;
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
				if ($rec=~ /^alt_id/){
					my @cols=split(/HP:/,$rec);
					$altid=$cols[1];
					$altid=~ /(0*)(\d*)/;
					$altid=$2;
					push(@{$altidhash{$id}},$altid);
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

my $altidline="INSERT INTO altid VALUES ";
foreach my $key (keys %altidhash){
	foreach my $altid (@{$altidhash{$key}}){
		$altidline.="($altid,$key),";
	}
}
$altidline =~ s/,$/;\n/;
push(@temp,$altidline);
push(@temp, "UPDATE HPOphenotype set HPOID =(select altid.newid from altid where altid.oldid=HPOphenotype.HPOID) where HPOID =(select altid.oldid from altid where altid.oldid=HPOphenotype.HPOID);\n");

push(@temp, "DROP table altid;\n");


#make file for graph_path,term
while (my $line = <SQL>){
	chomp($line);
	if ($line =~ /^INSERT INTO `graph_path` VALUES*/){
		push(@temp, "LOCK TABLES `graph_path_temp` WRITE;\n");
		$line =~ s/graph_path/graph_path_temp/g;
		push(@temp, "$line\n");
		push(@temp, "UNLOCK TABLES;\n");
        }
	if ($line =~ /^INSERT INTO `term` VALUES*/){
                push(@temp, "-- HPO term table\n");
		#push(@temp, "LOCK TABLES `term_temp` WRITE;\n");
		push(@temp, "LOCK TABLES `hpoterm_temp` WRITE;\n");
		procterm($line);
		push(@temp,"UNLOCK TABLES;\n");
	}
}
foreach my $line(@temp){
	print UPDATESQL $line;
}
foreach my $line(@update){
	print UPDATESQL $line;
}

close TEMPSQL;
close UPDATESQL;

sub uniq {
	return keys %{{ map { $_ => 1 } @_ }};
}



sub procterm(){
	 my $line=$_[0];
        $line =~ s/INSERT INTO `term` VALUES //g;
        my @cols=split(/\),\(/,$line);
	my $newterm="INSERT INTO hpoterm_temp VALUES";
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
		$newterm.="($1,$2,$3,$4,$5,'$cmt',$7,'$def','$synonym'),";
	
        }
	$newterm =~ s/,$/;\n/;
	push (@temp,$newterm);
}
