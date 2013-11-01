package WebService::Amazon::DynamoDB;
# ABSTRACT: Abstract API support for Amazon DynamoDB
use strict;
use warnings;

our $VERSION = '0.001';

=head1 NAME

WebService::Amazon::DynamoDB -

=head1 SYNOPSIS

=head1 DESCRIPTION

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

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2011. Licensed under the same terms as Perl itself.

