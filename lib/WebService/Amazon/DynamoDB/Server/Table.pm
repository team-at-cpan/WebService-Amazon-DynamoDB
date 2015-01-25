package WebService::Amazon::DynamoDB::Server::Table;

use strict;
use warnings;

use Future;
use Future::Utils qw(repeat);

use WebService::Amazon::DynamoDB::Server::Item;

use constant DYNAMODB_INDEX_OVERHEAD => 100;

=head2 new

=cut

sub new { my $class = shift; bless {@_}, $class }

=head2 name

=cut

sub name { shift->{TableName} // die 'invalid table - no name' }

=head2 item_by_id

=cut

sub item_by_id {
	my ($self, @id) = @_;
	my $k = $self->key_for_id(@id) // return Future->fail('bad key');
	exists $self->{items}{$k} or return Future->fail('item not found');
	Future->done($self->{items}{$k});
}

=head2 key_for_id

=cut

sub key_for_id {
	my ($self, @id) = @_;
	join "\0", map Encode::encode("UTF-8", $_), @id;
}

sub bytes_used {
	my ($self) = @_;
	$self->{bytes_used} //= do {
		my $total = 0;
		(repeat {
			shift->bytes_used->on_done(sub {
				$total += DYNAMODB_INDEX_OVERHEAD + shift
			})
		} foreach => [ @{$self->{items}} ],
		  otherwise => sub { Future->done($total) })
	}
}

1;

