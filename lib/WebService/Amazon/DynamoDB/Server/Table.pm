package WebService::Amazon::DynamoDB::Server::Table;

use strict;
use warnings;

use Future;

sub new { my $class = shift; bless {@_}, $class }

sub name { shift->{TableName} // die 'invalid table - no name' }

sub item_by_id {
	my ($self, @id) = @_;
	my $k = $self->key_for_id(@id) // return Future->fail('bad key');
	exists $self->{items}{$k} or return Future->fail('item not found');
	Future->done($self->{items}{$k});
}

sub key_for_id {
	my ($self, @id) = @_;
	join "\0", map Encode::encode("UTF-8", $_), @id;
}

1;

