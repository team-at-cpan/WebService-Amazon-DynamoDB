package WebService::Amazon::DynamoDB::20120810;
use strict;
use warnings;

=head1 NAME

Net::Async::DynamoDB::20120810 - interact with DynamoDB using API version 20120810

=head1 SYNOPSIS

=head1 DESCRIPTION

=cut

use Future;
use Future::Utils qw(repeat);
use POSIX qw(strftime);
use JSON::XS;
use Scalar::Util qw(reftype);
use B qw(svref_2object);

use WebService::Amazon::Signature;

my $json = JSON::XS->new;

sub new {
	my $class = shift;
	bless { @_ }, $class
}

sub implementation { shift->{implementation} }
sub host { shift->{host} }
sub port { shift->{port} }
sub algorithm { 'AWS4-HMAC-SHA256' }
sub scope { '20110909/us-east-1/host/aws4_request' }
sub access_key { shift->{access_key} }
sub secret_key { shift->{secret_key} }

=head2 create_table

Creates a new table. It may take some time before the table is marked
as active - use L</wait_for_table> to poll until the status changes.

=cut

sub create_table {
	my $self = shift;
	my %args = @_;
	my %payload = (
		TableName => $args{table},
		ProvisionedThroughput => {
			ReadCapacityUnits => $args{read_capacity} || 5,
			WriteCapacityUnits => $args{write_capacity} || 5,
		}
	);
	my @fields = @{$args{fields}};
	my %field;
	while(my ($k, $type) = splice @fields, 0, 2) {
		$field{$k} = $type;
		push @{$payload{AttributeDefinitions} }, {
			AttributeName => $k,
			AttributeType => $type || 'S',
		}
	}
	my @primary = @{$args{primary} || []};
	while(my ($k, $type) = splice @primary, 0, 2) {
		die "Unknown field $k" unless exists $field{$k};
		push @{$payload{KeySchema} }, {
			AttributeName => $k,
			KeyType       => $type || 'HASH',
		}
	}
	my $req = $self->make_request(
		target => 'CreateTable',
		payload => \%payload,
	);
	$self->_request($req)
}

=head2 describe_table

Describes the given table.

=cut

sub describe_table {
	my $self = shift;
	my %args = @_;
	my %payload = (
		TableName => $args{table},
	);
	my $req = $self->make_request(
		target => 'DescribeTable',
		payload => \%payload,
	);
	$self->_request($req)->transform(
		# Sadly not the same key as used in DeleteTable
		done => sub { my $content = shift; $json->decode($content)->{Table}; }
	);
}

=head2 delete_table

Delete a table entirely.

=cut

sub delete_table {
	my $self = shift;
	my %args = @_;
	my %payload = (
		TableName => $args{table},
	);
	my $req = $self->make_request(
		target => 'DeleteTable',
		payload => \%payload,
	);
	$self->_request($req)->transform(
		# Sadly not the same key as used in DescribeTable
		done => sub { my $content = shift; $json->decode($content)->{TableDescription} }
	);
}

=head2 wait_for_table

Waits for the given table to be marked as active.

=cut

sub wait_for_table {
	my $self = shift;
	my %args = @_;
	repeat {
		$self->describe_table(%args)
	} until => sub {
		my $f = shift;
		my $status = $f->get->{TableStatus};
		warn "status: " . $status; 
		$status eq 'ACTIVE'
	};
}

=head2 each_table

Run code for all current tables.

=cut

sub each_table {
	my $self = shift;
	my $code = shift;
	my %args = @_;
	my %payload;
	my $last_table;
	repeat {
		$payload{ExclusiveStartTableName} = $args{start} if defined $args{start};
		$payload{Limit} = $args{limit} if defined $args{limit};
		my $req = $self->make_request(
			target => 'ListTables',
			payload => \%payload,
		);
		$self->_request($req)->on_done(sub {
			my $rslt = shift;
			my $data = $json->decode($rslt);
			for my $tbl (@{$data->{TableNames}}) {
				$code->($tbl);
			}
			$last_table = $data->{LastEvaluatedTableName};
			$args{start} = $last_table;
		});
	} while => sub {
#		warn "Checking @_ => $last_table\n";
		defined $last_table
	};
}

=head2 put_item

Writes a single item to the table.

=cut

sub put_item {
	my $self = shift;
	my %args = @_;

	my %payload = (
		TableName => $args{table},
		ReturnConsumedCapacity => $args{capacity} ? 'TOTAL' : 'NONE',
	);
	foreach my $k (keys %{$args{fields}}) {
		my $v = $args{fields}{$k};	
		$payload{Item}{$k} = { type_for_value($v) => $v };
	}

	my $req = $self->make_request(
		target => 'PutItem',
		payload => \%payload,
	);
	$self->_request($req)->transform(
		# Sadly not the same key as used in DeleteTable
		done => sub { $json->decode(shift)->{Table}; }
	);

}

=head2 batch_get_item

Retrieve a batch of items from one or more tables.

=cut

sub batch_get_item {
	my $self = shift;
	my $code = shift;
	my %args = @_;
	my %payload = (
		ReturnConsumedCapacity => $args{capacity} ? 'TOTAL' : 'NONE',
	);
	for my $tbl (keys %{$args{items}}) {
		my $item = $args{items}{$tbl};
		my @keys = @{$item->{keys}};
		$payload{RequestItems}{$tbl}{Keys} = [];
		while(my ($k, $v) = splice @keys, 0, 2) {
			push @{$payload{RequestItems}{$tbl}{Keys}}, {
				$k => {
					type_for_value($v) => $v
				}
			};
		}
	}

	my $finished = 0;
	repeat {
		my $req = $self->make_request(
			target => 'BatchGetItem',
			payload => \%payload,
		);
		$self->_request($req)->on_done(sub {
			my $rslt = shift;
			my $data = $json->decode($rslt);
			my @resp = %{$data->{Responses}};
			# { Something => [ { Name => { S => 'text' } } ] }
			while(my ($k, $v) = splice @resp, 0, 2) {
				for my $entry (@$v) {
					$code->($k => {
						map {; $_ => values %{$entry->{$_}} } keys %$entry
					});
				}
			}
			$args{RequestItems} = $data->{UnprocessedKeys};
			$finished = 1 unless keys %{$data->{UnprocessedKeys}};
		});
	} until => sub { $finished };
}

=head2 scan

Scan a table for values with an optional filter expression.

=cut

sub scan {
	my $self = shift;
	my $code = shift;
	my %args = @_;
	my %payload = (
		TableName => $args{table},
		ReturnConsumedCapacity => $args{capacity} ? 'TOTAL' : 'NONE',
	);
	$payload{AttributesToGet} = $args{fields};
	$payload{Limit} = $args{limit} if exists $args{limit};
	my %filter;
	for my $f (@{$args{filter}}) {
		$filter{$f->{field}} = {
			AttributeValueList => [ {
				type_for_value($f->{value}) => $f->{value},
			} ],
			ComparisonOperator => $f->{compare} || 'EQ',
		}
	}
	$payload{ScanFilter} = \%filter if %filter;
	my $finished = 0;
	my $count = 0;
	repeat {
		my $req = $self->make_request(
			target => 'Scan',
			payload => \%payload,
		);
		$self->_request($req)->on_done(sub {
			my $rslt = shift;
			my $data = $json->decode($rslt);
			for my $entry (@{$data->{Items}}) {
				$code->({
					map {; $_ => values %{$entry->{$_}} } keys %$entry
				});
			}
			$count += $data->{Count};
			$args{ExclusiveStartKey} = $data->{LastEvaluatedKey};
			$finished = 1 unless keys %{$data->{LastEvaluatedKey}};
		});
	} until => sub { $finished };
}

=head1 METHODS - Internal

The following methods are intended for internal use and are documented
purely for completeness - for normal operations see L</METHODS> instead.

=head2 make_request

Generates an L<HTTP::Request>.

=cut

sub make_request {
	my $self = shift;
	my %args = @_;
	my $api_version = '20120810';
	my $host = $self->host;
	my $target = $args{target};
	my $js = JSON::XS->new;
	my $req = HTTP::Request->new(
		POST => 'http://' . $self->host . ($self->port ? (':' . $self->port) : '') . '/'
	);
	$req->header( host => $host );
	my $http_date = strftime('%a, %d %b %Y %H:%M:%S %Z', localtime);
	$req->protocol('HTTP/1.1');
	$req->header( 'Date' => $http_date );
	$req->header( 'x-amz-target', 'DynamoDB_'. $api_version. '.'. $target );
	$req->header( 'content-type' => 'application/x-amz-json-1.0' );
	my $payload = $js->encode($args{payload});
	$req->content($payload);
	$req->header( 'Content-Length' => length($payload));
	my $amz = WebService::Amazon::Signature->new(
		version    => 4,
		algorithm  => $self->algorithm,
		access_key => $self->access_key,
		scope      => $self->scope,
		secret_key => $self->secret_key,
	);
	$amz->from_http_request($req);
	$req->header(Authorization => $amz->calculate_signature);
	$req
}

sub _request {
	my $self = shift;
	my $req = shift;
	$self->implementation->request($req)
}

sub ua {
	my $self = shift;
	unless($self->{ua}) {
		my $ua = Net::Async::HTTP->new;
		$self->loop->add($ua);
		$self->{ua} = $ua;
	}
	$self->{ua};
}

=head1 FUNCTIONS - Internal

=head2 type_for_value

Returns an appropriate type (N, S, SS etc.) for the given
value.

Rules are similar to L<JSON> - if you want numeric, numify (0+$value),
otherwise you'll get a string.

=cut

sub type_for_value {
	my $v = shift;
	if(my $ref = reftype($v)) {
		# An array maps to a sequence
		if($ref eq 'ARRAY') {
			my $flags = B::svref_2object(\$v)->FLAGS;
			# Any refs mean we're sending binary data
			return 'BS' if grep ref($_), @$v;
			# Any stringified values => string data
			return 'SS' if grep $_ & B::SVp_POK, map B::svref_2object(\$_)->FLAGS, @$v;
			# Everything numeric? Send as a number
			return 'NS' if @$v == grep $_ & (B::SVp_IOK | B::SVp_NOK), map B::svref_2object(\$_)->FLAGS, @$v;
			# Default is a string sequence
			return 'SS';
		} else {
			return 'B';
		}
	} else {
		my $flags = B::svref_2object(\$v)->FLAGS;
		return 'S' if $flags & B::SVp_POK;
		return 'N' if $flags & (B::SVp_IOK | B::SVp_NOK);
		return 'S';
	}
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2013. Licensed under the same terms as Perl itself.

