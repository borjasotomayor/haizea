BEGIN	{
    count = 0
}

NR == 1	{
    timeBegin = $1 + 0
    prevTime = 0
}

$4 ~ logEvent {
    time = $1 - timeBegin;

    if ( time == prevTime )
    {
	count++
    }
    else
    {
	printf "%f %d\n", time,++count
	prevTime = time
    }
}