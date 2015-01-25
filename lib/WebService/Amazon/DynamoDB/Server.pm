package WebService::Amazon::DynamoDB::Server;

use strict;
use warnings;

use Future;
use List::Util qw(min);
use List::UtilsBy qw(extract_by);

sub new { my $class = shift; bless {@_}, $class }

sub add_table {
	my ($self, %args) = @_;
	push @{$self->{tables}}, \%args;
	$self->{table_map}{$args{name}} = \%args;
	$self
}

sub drop_table {
	my ($self, %args) = @_;
	my $name = $args{name};
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

	my @names = sort map $_->{name}, @{$self->{tables}};
	if(exists $args{ExclusiveStartTableName}) {
		return Future->fail(
			'table ' . $args{ExclusiveStartTableName} . ' not found', 
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

sub have_table {
	my ($self, $name) = @_;
	return exists $self->{table_map}{$name};
}

1;

