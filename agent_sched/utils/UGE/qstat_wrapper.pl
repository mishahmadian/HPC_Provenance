#!/usr/bin/perl
##|==================================================|
##|         High Performance Computing Center        |
##|               Texas Tech University              |
##|                                                  |
##| This script can be used as an alias for "qstat"  |
##| command to fix the output of "qstat -j" by       |
##| replacing the h_vmem value with its original one.|
##|  * This script has no effect on other qstat      |
##|    options. (only work with qstat -j)            |
##|                                                  |
##|                                                  |
##| misha.ahmadian@ttu.edu                           |
##|==================================================|
#
use strict;
use warnings;
no warnings qw/uninitialized/;
#------ Variables --------#
my $max_num_cores = 36;
my $uge_bin = "/export/uge/bin/lx-amd64";
#-------------------------#
# Execute the UGE qstat and capture the output for further process
my $qstat_output = `$uge_bin/qstat @ARGV 2>&1`;
#
# Check if qstat is being called by "-j". If not then no need for
# further process
if (! grep(/^-j$/, @ARGV)) {
	print STDOUT "$qstat_output";
	exit 0;
}
# UGE qstat can be called with multiple job_ids
# e.g. qstat -j 123,124,125
# The output will be separated by (===) line
my $qstat_delim = (split /\n/, $qstat_output)[0];
# Separate the output for each job info
my @qstat_list = split /$qstat_delim/, $qstat_output;
# remove the first element since it's always empty
shift(@qstat_list);
#
# Now, for each job info correct the outputs for h_vmem
foreach my $qstat_info (@qstat_list) {
	# put the qstat output into an array split each line
	my @lines = split /\n/, $qstat_info;

	# Get the requested number of cores for this job
	my $cores = get_num_cores(@lines);

	# Always print the delimiter line to separate the jobs
	print STDOUT "$qstat_delim\n";

	# Continue if $cores was found
	if ($cores) {
		#
		# Find the h_vmem and replcae the value with actual value
		#
		foreach my $line (@lines) {
			if ($line =~ /(h_vmem=)(\d+)(\.\d+)*(\w*)/) {
				my $mem_size = $2;
				my $mem_float = $3;
				my $mem_unit = $4;

				# If h_vmem contains floating point, we will take it into account
				if ($mem_float) {
					$mem_size .= $mem_float;
				}
				my $orig_h_vmem = get_orig_hvmem($mem_size, $cores)."$mem_unit";
				$mem_size .= $mem_unit;
				# shows the adjusted value just for root user
				if ("$ENV{'USER'}" eq "root") {
					$orig_h_vmem .= " --> (adjusted to: $mem_size)";
				}
				# replace the original h_vmem (and adjustment for root) with h_vmem
				$line =~ s/$mem_size/$orig_h_vmem/g;
			}
			print STDOUT $line."\n";
		}
	}
	# In case of #cores was not found, just print everything
	# This is very rare!
	else {
			print STDOUT $qstat_info;
	}
}

#
# Find the number of cores from "qstat -j" output to cacluate the actual "h_vmem"
#
sub get_num_cores {
	my @rev_lines = reverse(@_);
	# The Parallel Environment information is at the end of the "qstat -j" output
	# Therefore, we start from bottom to search for number of cores
	foreach my $revline (@rev_lines) {
		if ($revline =~ /(parallel environment:.*:\s*)(\d+)/) {
			my $cores = $2;
			if ($cores > $max_num_cores) {
				$cores = $max_num_cores;
			}
			return $cores;
		}
	}
	return 0;
}
#
# Calculate the actual h_vmem based on current h_vmem and # of cores
#
sub get_orig_hvmem {
	my ($h_vmem, $cores) = @_;
	return sprintf "%.2f", (($h_vmem * ($cores + 1)) / $cores);
}
