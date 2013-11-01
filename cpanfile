requires 'parent', 0;
requires 'curry', 0;
requires 'Future', '>= 0.15';
requires 'JSON::XS', 0;
requires 'Mixin::Event::Dispatch', '>= 1.004';
requires 'WebService::Amazon::Signature', 0;
requires 'Module::Load', 0;
requires 'HTTP::Request', 0;

on 'test' => sub {
	requires 'Test::More', '>= 0.98';
	requires 'Test::Fatal', '>= 0.010';
};

