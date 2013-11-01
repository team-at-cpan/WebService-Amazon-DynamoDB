package WebService::Amazon::DynamoDB::NaHTTP;
use strict;
use warnings;

=head1 NAME

WebService::Amazon::DynamoDB::NaHTTP - make requests using L<Net::Async::HTTP>

=head1 DESCRIPTION

Provides a L</request> method which will use L<Net::Async::HTTP> to make
requests and return a L<Future> containing the result. Used internally by
L<WebService::Amazon::DynamoDB>.

=cut

use Future;
use Net::Async::HTTP 0.30;

=head2 new

Instantiate.

=cut

sub new { my $class = shift; bless {@_}, $class }

=head2 request

Issues the request. Expects a single L<HTTP::Request> object,
and returns a L<Future> which will resolve to the decoded
response content on success, or the failure reason on failure.

=cut

sub request {
	my $self = shift;
	my $req = shift;
	my ($host, $port) = split /:/, ''.$req->uri->host_port;
	$self->ua->do_request(
		request => $req,
		host    => $host,
		port    => $port || 80,
	)->transform(
		done => sub {
			shift->decoded_content
		},
	);
}

=head2 ua

Returns a L<Net::Async::HTTP> instance.

=cut

sub ua {
	my $self = shift;
	unless($self->{ua}) {
		my $ua = Net::Async::HTTP->new(
			max_connections_per_host => 0,
			user_agent               => 'PerlWebServiceAmazonDynamoDB/' . $self->VERSION,
			pipeline                 => 0,
			fail_on_error            => 1,
		);
		$self->loop->add($ua);
		$self->{ua} = $ua;
	}
	$self->{ua};
}

sub loop { shift->{loop} }

1;

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2012-2013. Licensed under the same terms as Perl itself.

