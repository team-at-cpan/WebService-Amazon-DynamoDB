use strict;
use warnings;

use Test::More;
use Test::Deep;
use Test::Fatal;

use Future;

use WebService::Amazon::DynamoDB::Server;
use Test::WebService::Amazon::DynamoDB::Server;

{
	my $srv = ddb_server {
		shift->create_table(
			TableName => 'test',
			AttributeDefinitions => [ {
				AttributeName => 'id',
				AttributeType => 'S',
			} ],
			KeySchema => [ {
				AttributeName => 'id',
				KeyType => 'HASH'
			} ],
			ProvisionedThroughput => {
				ReadCapacityUnits => "5",
				WriteCapacityUnits => "5",
			}
		)
	};
	my $describe_table_events = 0;
	$srv->bus->subscribe_to_event(
		describe_table => sub {
			my ($ev, $req, $rslt, $tbl) = @_;
			++$describe_table_events;
			isa_ok($req, 'HASH') or note explain $req;
			isa_ok($rslt, 'Future') or note explain $rslt;
			ok($rslt->is_ready, '... and it is ready');
			if($rslt->failure) {
				is($tbl, undef, 'undef table on failure');
				like($rslt->failure, qr/Exception/, 'had the word "exception" somewhere');
			} else {
				isa_ok($tbl, 'WebService::Amazon::DynamoDB::Server::Table') or note explain $tbl;
			}
		}
	);
	ok($srv->have_table('test'), 'have starting table');

	like(exception {
		$srv->describe_table(
		)->get;
	}, qr/ResourceNotFoundException/, 'exception with no table name');
	like(exception {
		$srv->describe_table(
			TableName => 'missing'
		)->get;
	}, qr/ResourceNotFoundException/, 'exception with non-existing table');
	like(exception {
		$srv->describe_table(
			TableName => 'test'
		)->get;
	}, qr/ResourceInUseException/, 'exception with table that is still being created');
	is(exception {
		$srv->table_status(test => 'ACTIVE')->get
	}, undef, 'mark table as active');
	is(exception {
		$srv->describe_table(
			TableName => 'test'
		)->get;
	}, undef, 'no exception on valid table');
	is($describe_table_events, 4, 'had correct number of events');
}

done_testing;

