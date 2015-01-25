package WebService::Amazon::DynamoDB::Server;

use strict;
use warnings;

use Future;
use List::Util qw(min);
use List::UtilsBy qw(extract_by);

sub new { my $class = shift; bless {@_}, $class }

sub add_table {
	my ($self, %args) = @_;
	$args{TableName} = delete $args{name} if exists $args{name};
	push @{$self->{tables}}, \%args;
	$self->{table_map}{$args{TableName}} = \%args;
	$self
}

sub drop_table {
	my ($self, %args) = @_;
	$args{TableName} = delete $args{name} if exists $args{name};
	my $name = $args{TableName};
	extract_by { $_ eq $name } @{$self->{tables}};
	delete $self->{table_map}{$name};
	$self
}

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

	$self->add_table(%args);
	Future->wrap;
}

sub have_table {
	my ($self, $name) = @_;
	return exists $self->{table_map}{$name};
}

1;

