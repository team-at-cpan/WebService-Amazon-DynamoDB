package WebService::Amazon::DynamoDB;
# ABSTRACT: Abstract API support for Amazon DynamoDB
use strict;
use warnings;

our $VERSION = '0.002';

=head1 NAME

WebService::Amazon::DynamoDB - support for the AWS DynamoDB API

=head1 SYNOPSIS

 my $ddb = WebService::Amazon::DynamoDB->new(
  implementation => 'WebService::Amazon::DynamoDB::LWP',
  version        => '20120810',
  access_key     => 'access_key',
  secret_key     => 'secret_key',
  host           => 'localhost',
  port           => 8000,
 );
 $ddb->batch_get_item(
  sub {
   my $tbl = shift;
   my $data = shift;
   warn "Batch get: $tbl had " . join(',', %$data) . "\n";
  },
  items => {
   $table_name => {
    keys => [
     name => 'some test name here',
    ],
    fields => [qw(name age)],
   }
  },
 )->get;

=head1 BEFORE YOU START

B<NOTE>: I'd recommend looking at the L<Amazon::DynamoDB> module first.
It is a fork of this one with better features, more comprehensive tests,
and overall it's maintained much more actively.

=head1 DESCRIPTION

Provides a L<Future>-based API for Amazon's DynamoDB REST API.
See L<WebService::Amazon::DynamoDB::20120810> for available methods.

Current implementations for issuing the HTTP requests:

=over 4

=item * L<WebService::UA::NaHTTP> - use L<Net::Async::HTTP>
for applications based on L<IO::Async> (this gives nonblocking behaviour)

=item * L<WebService::UA::LWP> - use L<LWP::UserAgent> (will
block, timeouts are unlikely to work)

=item * L<WebService::UA::MojoUA> - use L<Mojo::UserAgent>,
should be suitable for integration into a L<Mojolicious> application (could
be adapted for nonblocking, although the current version does not do this).

=back

Only the L<Net::Async::HTTP> implementation has had any significant testing or use.

=cut

use WebService::Amazon::DynamoDB::20120810;
use Module::Load;

=head1 METHODS

=cut

sub new {
	my $class = shift;
	my %args = @_;
	$args{implementation} //= __PACKAGE__ . '::LWP';
	unless(ref $args{implementation}) {
		Module::Load::load($args{implementation});
		$args{implementation} = $args{implementation}->new;
	}
	my $version = delete $args{version} || '201208010';
	my $pkg = __PACKAGE__ . '::' . $version;
	if(my $code = $pkg->can('new')) {
		$class = $pkg if $class eq __PACKAGE__;
		return $code->($class, %args)
	}
	die "No support for version $version";
}

1;

__END__

=head1 SEE ALSO

=over 4

=item * L<Net::Amazon::DynamoDB> - supports the older (2011) API with v2
signing, so it doesn't work with L<DynamoDB Local|http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html>.

=item * L<AWS::CLIWrapper> - alternative approach using wrappers around AWS
commandline tools

=back

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2013. Licensed under the same terms as Perl itself.

