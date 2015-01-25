use strict;
use warnings;

use Test::More;
use Test::Deep;
use Test::Fatal;

use Future;
use Future::Utils qw(fmap repeat call);

use WebService::Amazon::DynamoDB::Server;
use Test::WebService::Amazon::DynamoDB::Server;

{
	my $srv = ddb_server { };

	like(exception {
		$srv->create_table->get
	}, qr/ValidationException/, 'exception when creating without AttributeDefinitions');

	like(exception {
		$srv->create_table(
			AttributeDefinitions => [],
		)->get
	}, qr/ValidationException/, 'exception when creating without KeySchema');

	like(exception {
		$srv->create_table(
			AttributeDefinitions => [],
			KeySchema => [],
		)->get
	}, qr/ValidationException/, 'exception when creating with empty KeySchema');

	like(exception {
		$srv->create_table(
			AttributeDefinitions => [],
			KeySchema => [ {
				AttributeName => 'id',
				KeyType => 'HASH'
			} ],
		)->get
	}, qr/ValidationException/, 'exception when creating with KeySchema referring to missing attribute');

	like(exception {
		$srv->create_table(
			AttributeDefinitions => [ {
				AttributeName => 'id',
				AttributeType => 'S',
			} ],
			KeySchema => [ {
				AttributeName => 'id',
				KeyType => 'HASH'
			} ],
		)->get
	}, qr/ValidationException/, 'exception when creating without ProvisionedThroughput');

	my %args = (
		AttributeDefinitions => [ {
			AttributeName => 'id',
			AttributeType => 'S',
		} ],
		KeySchema => [ {
			AttributeName => 'id',
			KeyType => 'HASH'
		} ],
	);

	$args{ProvisionedThroughput} = {
		ReadCapacityUnits => "5",
		WriteCapacityUnits => "5",
	};
	like(exception {
		$srv->create_table(
			%args,
		)->get
	}, qr/ValidationException/, 'exception when creating without TableName');

	$args{TableName} = 'test_table';
	is(exception {
		$srv->create_table(
			%args,
		)->get
	}, undef, 'no exception when creating with all required parameters');

	ok($srv->have_table('test_table'), 'table is now found');
}

done_testing;

