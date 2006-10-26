<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>

<xsl:template match="run-experiments">
<html>
<head>
	<title>Experiment results</title>
	<meta http-equiv="Content-Type" content="text/html" />
	<meta http-equiv="Content-Language" content="en"/>
	<link rel="stylesheet" type="text/css" href="experiments.css" media="screen" />
</head>

<body>
<h1>Experiment results</h1>
<hr/>
<p>
<strong>Experiment date:</strong> <xsl:value-of select="@runDate"/>
</p>
<p>
<strong>Report generation date:</strong> <xsl:value-of select="@genDate"/>
</p>
<p>
<a href="traces/">Trace files and stats</a>
</p>
<hr/>

<xsl:apply-templates select="defaultconfig"/>

<hr/>

<h5>Experiments</h5>
<xsl:apply-templates select="experiments" mode="toc"/>
<xsl:if test="graphs">
	<h5>Common graphs</h5>
	<xsl:apply-templates select="graphs" mode="toc"/>
</xsl:if>

<hr/>

<xsl:apply-templates select="experiments"/>

<h3>Common Graphs</h3>
<xsl:apply-templates select="graphs"/>

</body>
</html>
</xsl:template>

<xsl:template match="defaultconfig">
<div class="defaultconfig">
<h3>Default configuration values</h3>
<xsl:apply-templates select="config"/>
</div>
</xsl:template>

<xsl:template match="config">
<p><strong><xsl:value-of select="@name"/></strong>: <xsl:value-of select="@value"/></p>
</xsl:template>

<xsl:template match="experiments" mode="toc">
<ul>
<xsl:for-each select="experiment-group">
<li><a href="#{@id}">Experiment #<xsl:number value="position()" format="1"/></a>: <xsl:value-of select="description"/></li>
</xsl:for-each>
</ul>
</xsl:template>

<xsl:template match="experiments">
<xsl:apply-templates select="experiment-group"/>
</xsl:template>

<xsl:template match="experiment-group">
<h3><a name="{@id}">Experiment #<xsl:number value="position()" format="1"/></a></h3>
<p><xsl:value-of select="description"/></p>
<p>Trace file: <tt><xsl:value-of select="trace"/></tt></p>
<table border="1" cellpadding="5">
<tr>
<th colspan="{count(experiment)}">Configurations</th>
</tr>
<tr>
<xsl:for-each select="experiment">
<th><xsl:value-of select="@label"/></th>
</xsl:for-each>
</tr>
<tr>
<xsl:for-each select="experiment">
<td><xsl:apply-templates select="configuration/config"/></td>
</xsl:for-each>
</tr>
</table>
<xsl:apply-templates select="graphs"/>
<hr/>
</xsl:template>


<xsl:template match="graphs" mode="toc">
<ul>
<xsl:for-each select="graph">
<li><a href="#{@graphDir}">Graph #<xsl:number value="position()" format="1"/></a>: <xsl:value-of select="@title"/></li>
</xsl:for-each>
</ul>
</xsl:template>

<xsl:template match="graphs">

<xsl:for-each select="graph">
<h4>Graph #<xsl:number value="position()" format="1"/></h4>
<p><xsl:value-of select="@title"/></p>
<div class="image">
<img src="{@graphDir}/graph.png"/>
</div>
<div class="graphdir">
[ <a href="{@graphDir}/">Graph file (GNUPlot file, data files, etc.)</a> ]
</div>


</xsl:for-each>

</xsl:template>


</xsl:stylesheet>
