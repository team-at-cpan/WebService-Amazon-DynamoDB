package WebService::Amazon::DynamoDB::Server;

use strict;
use warnings;

=head1 NAME

WebService::Amazon::DynamoDB - support for the AWS DynamoDB API

=head1 DESCRIPTION

=cut

use Encode;
use Future;
use List::Util qw(min);
use List::UtilsBy qw(extract_by);
use Time::Moment;

use WebService::Amazon::DynamoDB::Server::Table;
use WebService::Amazon::DynamoDB::Server::Item;

=head1 METHODS

=cut

=head2 new

=cut

sub new { my $class = shift; bless {@_}, $class }

=head2 list_tables

Takes the following named parameters:

=over 4

=item * ExclusiveStartTableName

=item * Limit

=back

Resolves to a hashref containing the following data:

=over 4

=item * LastEvaluatedTableName

=item * TableNames

=back

=cut

sub list_tables {
	my ($self, %args) = @_;

	my @names = sort map $_->{TableName}, @{$self->{tables}};
	if(exists $args{ExclusiveStartTableName}) {
		return Future->fail(
			'ValidationException: table ' . $args{ExclusiveStartTableName} . ' not found', 
		) unless $self->have_table($args{ExclusiveStartTableName});

		shift @names while @names && $names[0] ne $args{ExclusiveStartTableName};
	}
	use constant LIST_TABLES_MAX => 100;
	my $limit = min(LIST_TABLES_MAX, $args{Limit} // ());
	my %result;
	if(@names > $limit) {
		($result{LastEvaluatedTableName}) = splice @names, $limit;
	}
	$result{TableNames} = \@names;
	Future->wrap(\%result)
}

sub create_table {
	my ($self, %args) = @_;

	return Future->fail(
		'ValidationException - no AttributeDefinitions found'
	) unless exists $args{AttributeDefinitions};

	return Future->fail(
		'ValidationException - no KeySchema found'
	) unless exists $args{KeySchema};

	return Future->fail(
		'ValidationException - empty KeySchema found'
	) unless @{$args{KeySchema}};

	return Future->fail(
		'ValidationException - too many items found in KeySchema'
	) if @{$args{KeySchema}} > 2;

	return Future->fail(
		'ValidationException - invalid KeyType, expected HASH'
	) unless ($args{KeySchema}[0]{KeyType} // '') eq 'HASH';

	return Future->fail(
		'ValidationException - invalid KeyType, expected RANGE'
	) if @{$args{KeySchema}} > 1 && ($args{KeySchema}[1]{KeyType} // '') ne 'RANGE';

	my %attr = map {; $_->{AttributeName} => $_ } @{$args{AttributeDefinitions}};
	return Future->fail(
		'ValidationException - attribute ' . $_ . ' not found in AttributeDefinitions'
	) for grep !exists $attr{$_}, map $_->{AttributeName}, @{$args{KeySchema}};

	return Future->fail(
		'ValidationException - no ProvisionedThroughput found'
	) unless exists $args{ProvisionedThroughput};

	return Future->fail(
		'ValidationException - no ProvisionedThroughput found'
	) unless exists $args{TableName};

	return Future->fail(
		'ResourceInUseException - this table exists already'
	) if $self->have_table($args{TableName});

	$args{TableStatus} = 'CREATING';
	$args{ItemCount} = 0;
	$args{TableSizeBytes} = 0;
	$args{CreationDateTime} = Time::Moment->now;
	$self->add_table(%args);
	Future->done({
		TableDescription => {
			%args,
			CreationDateTime => $args{CreationDateTime}->to_string,
		}
	});
}

sub describe_table {
	my ($self, %args) = @_;

	my $name = delete $args{TableName};
	$self->validate_table_state($name => 'ACTIVE')->then(sub {
		Future->done({
			Table => $self->{table_map}{$name}
		})
	})
}

sub update_table {
	my ($self, %args) = @_;

	my $name = delete $args{TableName};
	$self->validate_table_state($name => 'ACTIVE')->then(sub {
		my $tbl = $self->{table_map}{$name};
		my %update;
		if(my $throughput = delete $args{ProvisionedThroughput}) {
			$update{ProvisionedThroughput}{$_} = $throughput->{$_} for grep exists $throughput->{$_}, qw(ReadCapacityUnits WriteCapacityUnits);
		}
		if(my $index = delete $args{GlobalSecondaryIndexUpdates}) {
			$update{GlobalSecondaryIndexUpdates}{$_} = $index->{$_} for keys %$index;
		}
		return Future->fail(
			'ValidationException - invalid keys provided'
		) if keys %args;
		for my $k (keys %update) {
			$tbl->{$k}{$_} = $update{$k}{$_} for keys %{$update{$k}};
		}
		$self->table_status($name => 'UPDATING')->transform(done => sub {
			+{
				TableDescription => $tbl
			}
		})
	})
}

sub delete_table {
	my ($self, %args) = @_;

	my $name = delete $args{TableName};
	$self->validate_table_state($name => qw(ACTIVE DELETING))->then(sub {
		return Future->fail(
			'ValidationException - invalid keys provided'
		) if keys %args;
		my $tbl = $self->{table_map}{$name};
		$self->table_status($name => 'DELETING')->transform(done => sub {
			+{
				TableDescription => $tbl
			}
		})
	})
}

sub put_item {
	my ($self, %args) = @_;
	my $name = delete $args{TableName};
	$self->validate_table_state($name => 'ACTIVE')->then(sub {
		my $tbl = $self->{table_map}{$name};

		my @id_fields = map $_->{AttributeName}, @{$tbl->{KeySchema}};
		return Future->fail(
			ValidationException =>
		) for grep !exists $args{Item}{$_}, @id_fields;

		my ($id) = join "\0", map values %{$args{Item}{$_}}, @id_fields;
		my $new = !exists $self->{data}{$name}{$id};
		$self->{data}{$name}{$id} = delete $args{Item};

		my %result;
		Future->needs_all(
			$self->return_values(delete $args{ReturnValues}),
			$self->consumed_capacity(delete $args{ReturnConsumedCapacity}),
			$self->collection_metrics(delete $args{ReturnItemCollectionMetrics}),
		)->then(sub {
			# Only add the keys if they were requested
			for(qw(Attributes ConsumedCapacity ItemCollectionMetrics)) {
				my $item = shift;
				$result{$_} = $item if defined $item
			}

			# Commit the changes
			++$tbl->{ItemCount} if $new;
			$tbl->{TableSizeBytes} += length Encode::decode('UTF-8', $id);

			Future->done(\%result);
		});
	})
}

=head2 METHODS - Internal

The following methods are not part of the standard DynamoDB public API,
so they are not recommended for use directly.

=cut

=head2 add_table

Adds this table - called by L</create_table> if everything passes validation.

=cut

sub add_table {
	my ($self, %args) = @_;
	$args{TableName} = delete $args{name} if exists $args{name};
	push @{$self->{tables}}, \%args;
	$self->{table_map}{$args{TableName}} = \%args;
	$self
}

=head2 drop_table

Drops the table - called to remove a table that was previously in 'DELETING' state.

=cut

sub drop_table {
	my ($self, %args) = @_;
	$args{TableName} = delete $args{name} if exists $args{name};
	my $name = $args{TableName};
	extract_by { $_ eq $name } @{$self->{tables}};
	delete $self->{table_map}{$name};
	Future->wrap
}

=head2 return_values

Resolves to the attributes requested for this update.

=cut

sub return_values {
	my ($self, $v) = @_;
	return Future->done(undef) if !defined($v) || $v eq 'NONE';
	if($v eq 'ALL_OLD') {
		return Future->done({ })
	} else {
		return Future->fail(
			ValidationException =>
		)
	}
}

=head2 consumed_capacity

Returns consumed capacity information if available.

=cut

sub consumed_capacity {
	my ($self, $v) = @_;
	return Future->done(undef) if !defined($v) || $v eq 'NONE';
	if($v eq 'INDEXES') {
		return Future->done({ })
	} elsif($v eq 'TOTAL') {
		return Future->done({ })
	} else {
		return Future->fail(
			ValidationException =>
		)
	}
}

=head2 collection_metrics

Resolves to collection metrics information, if available.

=cut

sub collection_metrics {
	my ($self, $v) = @_;
	return Future->done(undef) if !defined($v) || $v eq 'NONE';
	if($v eq 'SIZE') {
		return Future->done({ })
	} else {
		return Future->fail(
			ValidationException =>
		)
	}
}

my %valid_table_status = map {; $_ => 1 } qw(CREATING DELETING UPDATING ACTIVE);

=head2 table_status

Update or return current table status.

=cut

sub table_status {
	my ($self, $name, $status) = @_;
	if(defined $status) {
		return Future->fail('bad status') unless exists $valid_table_status{$status};
		$self->{table_map}{$name}{TableStatus} = $status
	}
	Future->done($self->{table_map}{$name}{TableStatus});
}

=head2 have_table

Returns true if we have this table.

=cut

sub have_table {
	my ($self, $name) = @_;
	return scalar exists $self->{table_map}{$name};
}

=head2 validate_table_state

Raises various exceptions based on table state.

=cut

sub validate_table_state {
	my ($self, $name, @allowed) = @_;

	return Future->fail(
		'ResourceNotFoundException'
	) unless defined $name;

	return Future->fail(
		'ResourceNotFoundException'
	) unless $self->have_table($name);

	my $status = $self->{table_map}{$name}{TableStatus};
	return Future->fail(
		'ResourceInUseException'
	) unless grep $status eq $_, @allowed;

	Future->done;
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2013. Licensed under the same terms as Perl itself.


