package Test::WebService::Amazon::DynamoDB::Server;

use strict;
use warnings;

use parent qw(Exporter);

BEGIN {
	our @EXPORT = our @EXPORT_OK = qw(
		fmap_over
		ddb_server
		add_table
	);
}

use WebService::Amazon::DynamoDB::Server;

use Test::More;
use Future::Utils qw(fmap repeat call);

our $SRV;

sub fmap_over(&;@) {
	my ($code, %args) = @_;
	my @result;
	(repeat {
		(shift || Future->done)->then(sub {
			my $last = shift;
			call {
				$code->($last)->on_done(sub {
					push @result, @_
				})
			}
		})
	} (exists $args{while}
		? (
			while => sub {
				!@_ || $args{while}->(shift->get)
			}
		) :()
	))->transform(done => sub {
		$args{map} ? (map $args{map}->($_), @result) : @result
	})
}

sub ddb_server(&;@) {
	my ($code) = shift;
	local $SRV = new_ok('WebService::Amazon::DynamoDB::Server');
	$code->($SRV);
	$SRV
}

sub add_table(@) {
	my %args = @_;
	$SRV->add_table(%args);
}

1;

