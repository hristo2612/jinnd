#!/usr/bin/env perl
# Reports every conditional-compilation construct in an integration test file
# that could stop it running on a platform we ship.
#
# Why this is not a grep. The round-1 guard matched one line-shaped form and
# claimed the class; a legal crate-level `#![cfg(target_os = "macos")]` walked
# straight through it. Deciding this needs the two things a line regex cannot
# do: an attribute is a BALANCED bracket group that may span lines and nest
# predicates arbitrarily deep, and the same bytes inside a comment or a string
# literal mean nothing at all. So the file is lexed — comments and literal
# bodies blanked, newlines preserved — and then every attribute and `cfg!`
# group is extracted whole and read as one normalized token stream.
#
# What it decides, stated exactly: the compiler's conditional-compilation
# surface — `#[...]`, `#![...]` at crate, module or item level, and `cfg!()`.
# That surface is closed and enumerable, so completeness over it is a claim
# this can actually keep. What it does NOT decide is arbitrary control flow: a
# test that reads `std::env::consts::OS` and returns early is ordinary program
# logic, and no source scanner settles that. That residual is named here rather
# than papered over, because an unnamed gap is exactly the round-1 defect.
use strict;
use warnings;

# Predicates that can differ across the platforms jinnd ships (macOS, Linux,
# Android), plus `windows`, which selects a platform we ship on none of.
# `unix` is deliberately absent: it is constant-true across all three, so it
# hides nothing. `feature`/`miri` are not platform predicates either.
my $PLATFORM = qr/\b(target_os|target_family|target_arch|target_env|target_vendor
                    |target_abi|target_pointer_width|target_endian|windows)\b/x;
my $SILENCED = qr/\bignore\b/;

# Replace every character with a space, keeping newlines so lines still count.
sub blanked {
    my ($text) = @_;
    $text =~ s/[^\n]/ /g;
    return $text;
}

# Blank comments and literal bodies in place, so what remains is only code.
sub lex_out_noncode {
    my ($src) = @_;
    my $out = '';
    my $i   = 0;
    my $n   = length $src;

    while ($i < $n) {
        my $two = substr($src, $i, 2);

        if ($two eq '/*') {    # block comments nest in Rust
            my ($depth, $j) = (0, $i);
            while ($j < $n) {
                my $here = substr($src, $j, 2);
                if ($here eq '/*') { $depth++; $j += 2; next; }
                if ($here eq '*/') { $depth--; $j += 2; last if $depth == 0; next; }
                $j++;
            }
            $out .= blanked(substr($src, $i, $j - $i));
            $i = $j;
            next;
        }
        if ($two eq '//') {
            my $j = index($src, "\n", $i);
            $j = $n if $j < 0;
            $out .= blanked(substr($src, $i, $j - $i));
            $i = $j;
            next;
        }
        if (substr($src, $i) =~ /^((?:b?r)(\#*)")/) {    # raw string
            my $terminator = '"' . $2;
            my $j = index($src, $terminator, $i + length($1));
            $j = ($j < 0) ? $n : $j + length($terminator);
            $out .= blanked(substr($src, $i, $j - $i));
            $i = $j;
            next;
        }
        if (substr($src, $i) =~ /^(b?)"/) {              # string
            my $j = $i + length($1) + 1;
            while ($j < $n && substr($src, $j, 1) ne '"') {
                $j += (substr($src, $j, 1) eq '\\') ? 2 : 1;
            }
            $j = ($j < $n) ? $j + 1 : $n;
            $out .= blanked(substr($src, $i, $j - $i));
            $i = $j;
            next;
        }
        if (substr($src, $i) =~ /^(b?'(?:\\.|[^\\'])')/) {    # char, not a lifetime
            $out .= blanked($1);
            $i += length($1);
            next;
        }

        $out .= substr($src, $i, 1);
        $i++;
    }
    return $out;
}

# Extract each attribute / cfg! group whole, following bracket depth.
sub conditional_groups {
    my ($code) = @_;
    my %closes = ('[' => ']', '(' => ')', '{' => '}');
    my @groups;

    while ($code =~ /(\#!?\[|\bcfg!\s*[\(\[\{])/g) {
        my $start    = $-[0];
        my $open_at  = $+[0] - 1;
        my $open     = substr($code, $open_at, 1);
        my $close    = $closes{$open};
        my ($depth, $j, $end) = (0, $open_at, -1);

        while ($j < length $code) {
            my $c = substr($code, $j, 1);
            $depth++ if $c eq $open;
            if ($c eq $close) { $depth--; if ($depth == 0) { $end = $j; last } }
            $j++;
        }
        last if $end < 0;    # unbalanced source; rustc will say so first

        my $text = substr($code, $start, $end - $start + 1);
        $text =~ s/\s+/ /g;
        push @groups, [$start, $text];
        pos($code) = $end + 1;
    }
    return @groups;
}

my $hits = 0;
for my $path (@ARGV) {
    open my $fh, '<', $path or die "scan: cannot read $path: $!\n";
    my $src = do { local $/; <$fh> };
    close $fh;

    my $code = lex_out_noncode($src);
    for my $group (conditional_groups($code)) {
        my ($offset, $text) = @$group;
        my $why =
              ($text =~ $PLATFORM) ? "platform predicate `$1`"
            : ($text =~ $SILENCED) ? 'silenced with `ignore`'
            :                        undef;
        next unless defined $why;

        my $line = 1 + (substr($code, 0, $offset) =~ tr/\n//);
        printf "%s:%d: %s — %s\n", $path, $line, $text, $why;
        $hits++;
    }
}

exit($hits ? 1 : 0);
