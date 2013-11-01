package WebService::Amazon::DynamoDB::LWP;
use strict;
use warnings;

=head1 NAME

WebService::Amazon::DynamoDB::LWP - make requests using L<LWP::UserAgent>

=head1 DESCRIPTION

Provides a L</request> method which will use L<LWP::UserAgent> to make
requests and return a L<Future> containing the result. Used internally by
L<WebService::Amazon::DynamoDB>.

=cut

use Future;
use LWP::UserAgent;

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
	my $resp = $self->ua->request($req);
	return Future->new->done($resp->decoded_content) if $resp->is_success;

	my $status = join ' ', $resp->code, $resp->message;
	return Future->new->fail($status, $resp, $req)
}

=head2 ua

Returns the L<LWP::UserAgent> instance.

=cut

sub ua { shift->{ua} ||= LWP::UserAgent->new }

1;

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2012-2013. Licensed under the same terms as Perl itself.

