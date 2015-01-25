use strict;
use warnings;

use Test::More;
use Test::Deep;
use Test::Fatal;

use Future;
use Future::Utils qw(fmap repeat call);

use WebService::Amazon::DynamoDB::Server;
use Test::WebService::Amazon::DynamoDB::Server;

{ # Simple list, no pagination
	my @tables = qw(first second third fourth fifth sixth);
	my $srv = ddb_server {
		add_table name => $_ for qw(first second third fourth fifth sixth);
	};

	my $list_table_events = 0;
	$srv->bus->subscribe_to_event(
		list_tables => sub {
			my ($ev, $req, $rslt, $tables) = @_;
			++$list_table_events;
			isa_ok($req, 'HASH') or note explain $req;
			isa_ok($rslt, 'Future') or note explain $rslt;
			ok($rslt->is_ready, '... and it is ready');
			if($rslt->failure) {
				is($tables, undef, 'undef tables on failure');
				like($rslt->failure, qr/Exception/, 'had the word "exception" somewhere');
			} else {
				isa_ok($tables, 'ARRAY');
				for(grep !$_->isa('WebService::Amazon::DynamoDB::Server::Table'), @$tables) {
					fail("unexpected entry in tables");
					note explain $_;
				}
			}
		}
	);
	is(exception {
		cmp_deeply($srv->list_tables->get->{TableNames}, bag(@tables), "have expected tables");
	}, undef, 'no exception when listing all tables');

	is(exception {
		cmp_deeply([
			(fmap_over {
				my $last = shift;
				$srv->list_tables(
					Limit => 2,
					($last && exists $last->{LastEvaluatedTableName})
					? (ExclusiveStartTableName => $last->{LastEvaluatedTableName})
					: ()
				)->on_done(sub {
					my $data = shift;
					ok(ref $data eq 'HASH', 'had a hash');
					ok(exists $data->{TableNames}, 'have TableNames key');
					is(@{$data->{TableNames}}, 2, 'only two items in the list');
				})
			} while => sub { exists shift->{LastEvaluatedTableName} },
			  map => sub { @{shift->{TableNames}} }
			)->get
		], bag(@tables), "have expected tables when paging");
	}, undef, 'no exception when listing paginated tables');

	# We should get some sort of error with an invalid starting table
	like(exception {
		$srv->list_tables(
			ExclusiveStartTableName => 'does_not_exist'
		)->get
	}, qr/ValidationException/, 'bad starting table name raises exception');

	is($list_table_events, 5, 'had expected number of events');
}

done_testing;

