BEGIN	{
    count = 0
    num = 0
}

NR == 1 {
    timeBegin = $1
}


$4 ~ beginEvent && $3 ~ tag {
    if (NR==1)
        timeBegin = $1
    time = $1 - timeBegin
    start[$3]=time
}


function updateValues()
{
    num++
    durationEvent = end[$3] - start[$3]
    ratio = durationEvent / duration
    count += ratio
    avg = count / num
    printf "%f %f %f\n", time, ratio, avg
}

$4 ~ endEvent && $3 ~ tag {
    time = $1 - timeBegin
    end[$3]=time
}

$4 ~ "VM_RUNNING" && $3 ~ tag  {
    time = $1 - timeBegin
    duration = $5
    updateValues()
}
