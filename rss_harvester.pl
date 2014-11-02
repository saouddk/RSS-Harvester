# Author: Saoud Khalifah <saouddk@gmail.com>
# Purpose: Harvest RSS articles and harvest them by keywords in MySQL database. 



use XML::RSS::Parser::Lite;
use URI::Escape;
use Lingua::EN::Keywords;
use LWP::Simple;
use DBI;
use HTML::Strip;
use HTML::StripTags qw(strip_tags);


#-------Database Vars------#
my $db_name:shared ="";
my $db_user:shared = "";
my $db_pass:shared = "";
my $db_host:shared ="";
#--------------------------#

my $dbh = DBI->connect("DBI:mysql:$db_name:$db_host", $db_user, $db_pass);

        
sub tokenizeKeywords{
	my ($url, $rss_id) = @_;        
        my $xml = get($url);
        my $rp = new XML::RSS::Parser::Lite;
        $rp->parse($xml);
        my $hs = HTML::Strip->new();
       
		my $cnt = getCount();
        for (my $i = 0; $i < $rp->count(); $i++) {
                my $it = $rp->get($i);
                if(!defined($it) || !defined($rp)){
                	next;
                }
                my $author = uri_unescape($it->get('author'));
                my $summary = uri_unescape($it->get('description'));
                my $title = uri_unescape($it->get('title'));
                my $date_article = uri_unescape($it->get('pubDate'));
                my $link = (defined $it->get('guid') ? uri_unescape($it->get('guid')) : uri_unescape($it->get('url')));
                if(length($title) <= 5 && length($summary) <=10){
                	next;
                }
                if(length($url)<=0){
                		next;	
                }
                $title = $hs->parse( $title );
                $summary = $hs->parse( $summary );
                $link = $hs->parse( $link );
				$title = strip_tags( $title, '' );
				$summary = strip_tags( $summary, '' );
				$title =~ s/\n//g;
				$summary =~ s/\n//g;
				$summary =~ s/[^[:ascii:]]+//g;
				$title =~ s/[^[:ascii:]]+//g;
                print "Author = $author \nSummary = $summary\nTitle = $title\nDate = $date_article\nLink = $link\nRSSID = $rss_id\n";

                my $query = "INSERT INTO article (article_id, author, summary, title, date, link, rss_id) VALUES ('".$cnt."', '".$author."', '".$summary."', '".$title."', '".$date_article."', '".$link."', '".$rss_id."')";
                my $sqlQuery = undef;
				my $rv = undef;
				if($dbh){
					$sqlQuery  = $dbh->prepare($query) or warn "Can't prepare $query: $dbh->errstr\n";;
				}
				if($dbh && $sqlQuery){
					$rv = $sqlQuery->execute;

					
					if($rv){
						$cnt++;
						my @row=();
						print "Tokenizing keywords!!\n";
						my @keywords = keywords($summary);
                
		        		foreach (@keywords) {
		 					print "Keyword from summary = ".$_."\n";
		 					my $cur_keyword = lc($_);
		 					if(!defined cur_keyword || length($cur_keyword) <= 1){
		 						next;
		 					}
		 					$query = "SELECT keyword_id from keyword WHERE k_name = '".$_."'";
		 					$sqlQuery  = $dbh->prepare($query) or warn "Can't prepare $query: $dbh->errstr\n";;
		 					$rv = $sqlQuery->execute;
		 					my $k_id = "";
		 					if($rv)
		 					{
		 						#Keyword already exists in keyword table
		 						
		 						@row= $sqlQuery->fetchrow_array();
								$k_id = $row[0];
								if(defined $k_id){
									print "Keyword already exists in table! Keyword = $cur_keyword, K_id = $k_id\n";
								}
		 					}
		 					if(!defined $k_id){
		 						$k_id = getCountKeyword()+1;
		 		
		 						$query = "INSERT INTO keyword (keyword_id, k_name) VALUES ('".$k_id."', '".$cur_keyword."')";
		 						$sqlQuery  = $dbh->prepare($query) or warn "Can't prepare $query: $dbh->errstr\n";;
		 						$rv = $sqlQuery->execute or warn "Can't execute $query: ".$sqlQuery->errstr."\n";
		 						if(!$rv){
	
		 							next;
		 						}
		 					}
		 					my $article_id = $cnt -1;
		 					my $summ = lc($summary);
		 					my $count_times = () = $summ =~ /$cur_keyword/g;
		 					if($count_times <= 0){
		 						next;
		 					}

		 					$query = "INSERT INTO article_keyword (article_id, keyword_id, k_count) VALUES ('".$article_id."', '".$k_id."', '".$count_times."')";
		 					$sqlQuery  = $dbh->prepare($query) or warn "Can't prepare $query: $dbh->errstr\n";
		 					$rv = $sqlQuery->execute or warn "Can't execute $query: $dbh->errstr\n";;
		 					if(!$rv){
		 						next;
		 					}
		 
		 					
		        		}
		        		@keywords = keywords($title);
		        		
		        		foreach (@keywords) {
		 					print "Keywords from title = ".$_."\n";
		 					my $cur_keyword = lc($_);
		 					if(!defined cur_keyword || length($cur_keyword) <= 1){
		 						next;
		 					}
		 					$query = "SELECT keyword_id from keyword WHERE k_name = '".$_."'";
		 					$sqlQuery  = $dbh->prepare($query) or warn "Can't prepare $query: $dbh->errstr\n";;
		 					$rv = $sqlQuery->execute;
		 					my $k_id = "";
		 					if($rv)
		 					{
		 						#Keyword already exists in keyword table
		 						
		 						@row= $sqlQuery->fetchrow_array();
								$k_id = $row[0];
								if(defined $k_id){
									print "Keyword already exists in table! Keyword = $cur_keyword, K_id = $k_id\n";
								}
		 					}
		 					if(!defined $k_id){
		 						$k_id = getCountKeyword()+1;
		
		 						$query = "INSERT INTO keyword (keyword_id, k_name) VALUES ('".$k_id."', '".$cur_keyword."')";
		 						$sqlQuery  = $dbh->prepare($query) or warn "Can't prepare $query: $dbh->errstr\n";;
		 						$rv = $sqlQuery->execute or warn "Can't execute $query: ".$sqlQuery->errstr."\n";
		 						if(!$rv){
		 							next;
		 						}
		 					}
		 					my $article_id = $cnt -1;
		 					my $summ = lc($summary);
		 					my $count_times = () = $summ =~ /$cur_keyword/g;
		 					if($count_times <= 0){
		 						$count_times = 1;
		 					}
		 					$query = "INSERT INTO article_keyword (article_id, keyword_id, k_count) VALUES ('".$article_id."', '".$k_id."', '".$count_times."')";
		 					$sqlQuery  = $dbh->prepare($query) or warn "Can't prepare $query: $dbh->errstr\n";
		 					$rv = $sqlQuery->execute or warn "Can't execute $query: $dbh->errstr\n";;
		 					if(!$rv){
		 						next;
		 					}
		 					print "Successfully added article_keyword for article id = $article_id and key_id = $k_id, counts = $count_times\n";
		        		}
					}
					
				}
	
				next;

        		
        		
        		
        }
        $hs->eof;
}
sub getCount{
	my $query = "SELECT count(*) from article";
	if(defined($dbh)){
	if(!$dbh->ping){
		$dbh = undef;
		while(!defined($dbh)){
			$dbh = DBI->connect("DBI:mysql:$db_name:$db_host", $db_user, $db_pass);
		}
	}
	}else{
		return;	
	}
	my $sqlQuery  = $dbh->prepare($query)
	or return;
	my $rv = $sqlQuery->execute
	or return;
	my @row= $sqlQuery->fetchrow_array();
	return $row[0];
}
sub getCountKeyword{
	my $query = "SELECT count(*) from keyword";
	if(defined($dbh)){
	if(!$dbh->ping){
		$dbh = undef;
		while(!defined($dbh)){
			$dbh = DBI->connect("DBI:mysql:$db_name:$db_host", $db_user, $db_pass);
		}
	}
	}else{
		return;	
	}
	my $sqlQuery  = $dbh->prepare($query)
	or return;
	my $rv = $sqlQuery->execute
	or return;
	my @row= $sqlQuery->fetchrow_array();
	return $row[0];
}
sub harvest{
	my $query = "SELECT rss_url from rss_source";
	my $sqlQuery  = $dbh->prepare($query)
	or return;
	my $rv = $sqlQuery->execute
	or return;
	my $i = 1;
	while (my @row= $sqlQuery->fetchrow_array()) {
		print $row[0]."\n";
		tokenizeKeywords($row[0], $i);
		$i++;
	}
	
}
 
 
harvest();		