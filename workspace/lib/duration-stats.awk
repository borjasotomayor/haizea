BEGIN	{
    count = 0
    num = 0
}

NR == 1 {
    timeBegin = $1
}


$4 ~ beginEvent {
    if (NR==1)
        timeBegin = $1
    time = $1 - timeBegin
    start[$3]=time
}

$4 ~ endEvent {
    if ($3 in start)
    {
    num++
    time = $1 - timeBegin
    duration = time - start[$3]
    count += duration
    avg = count / num
    printf "%f %f %f %f\n",  time, avg, duration, count
    delete start[$3]
    }
}
