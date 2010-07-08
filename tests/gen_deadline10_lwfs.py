from mx.DateTime import TimeDelta

A_prematureends = [True, False]
BC_prematureends = [True, False]
BC_requesttimes = [(TimeDelta(minutes=15), TimeDelta(minutes=20)),
                   (TimeDelta(minutes=40), TimeDelta(minutes=45)),
                   (TimeDelta(minutes=45), TimeDelta(minutes=105))]

numfile = 1

for A_prematureend in A_prematureends:
    for BC_prematureend in BC_prematureends:
        for (B_requesttime, C_requesttime) in BC_requesttimes:
            f = open ("deadline10-%i.lwf" % numfile, "w")
            
            print >> f, """<?xml version="1.0"?>
<lease-workload name="deadline10">
  <description>
  """

            if A_prematureend:
                print >> f, "First lease ends prematurely."
            else:
                print >> f, "First lease does not end prematurely."

            if BC_prematureend:
                print >> f, "Second and third lease end prematurely."
            else:
                print >> f, "Second and third lease do not end prematurely."

            print >> f, "Second and third lease start at %s and %s" % (B_requesttime, C_requesttime)
            
            print >> f, """
  </description>

  <site>
    <resource-types names="CPU Memory"/>
    <nodes>
      <node-set numnodes="4">
        <res type="CPU" amount="100"/>
        <res type="Memory" amount="1024"/>
      </node-set>
    </nodes>
  </site>
  <lease-requests>

    <lease-request arrival="00:00:00.00">"""
            if A_prematureend:
                 print >> f, """<realduration time="01:30:00.00"/>"""
                 
            print >> f, """
      <lease id="1" preemptible="true">
        <nodes>
          <node-set numnodes="4">
            <res amount="100" type="CPU"/>
            <res amount="1024" type="Memory"/>
          </node-set>
        </nodes>
        <start>
          <exact time="00:30:00.00"/>
        </start>
        <duration time="02:00:00.00"/>
        <deadline time="10:00:00.00"/>
        <software>
          <disk-image id="foobar1.img" size="1024"/>
        </software>
      </lease>
    </lease-request>
    <lease-request arrival="%s">""" % B_requesttime
            if BC_prematureend:
                 print >> f, """<realduration time="00:10:00.00"/>"""
                 
            print >> f, """
      <lease id="2" preemptible="true">
        <nodes>
          <node-set numnodes="4">
            <res amount="100" type="CPU"/>
            <res amount="1024" type="Memory"/>
          </node-set>
        </nodes>
        <start>
          <exact time="01:00:00.00"/>
        </start>
        <duration time="00:15:00.00"/>
        <deadline time="01:15:00.00"/>
        <software>
          <disk-image id="foobar1.img" size="1024"/>
        </software>
      </lease>
    </lease-request>
    <lease-request arrival="%s">""" % C_requesttime
            if BC_prematureend:
                 print >> f, """<realduration time="00:10:00.00"/>"""
                 
            print >> f, """
      <lease id="3" preemptible="true">
        <nodes>
          <node-set numnodes="4">
            <res amount="100" type="CPU"/>
            <res amount="1024" type="Memory"/>
          </node-set>
        </nodes>
        <start>
          <exact time="02:00:00.00"/>
        </start>
        <duration time="00:15:00.00"/>
        <deadline time="02:15:00.00"/>
        <software>
          <disk-image id="foobar1.img" size="1024"/>
        </software>
      </lease>
    </lease-request>
  </lease-requests>
</lease-workload>"""

            f.close()
            print "    def test_deadline10_%i(self):" % numfile
            print "        self._tracefile_test(\"deadline10-%i.lwf\")" % numfile
            print "        self._verify_done([1,2,3])"
            print
            numfile += 1