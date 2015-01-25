package WebService::Amazon::DynamoDB::Server::Table;

use strict;
use warnings;

sub new { my $class = shift; bless {@_}, $class }

sub name { shift->{TableName} }

1;

