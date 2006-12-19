%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Craig Wiegert's modifications to johnchithesis.cls 
% (updated 26 Jan 2006)
% Send comments/changes/questions to <cwiegert@alumni.uchicago.edu>.
% 
% NOTE: Be sure to check your margins carefully on your hardcopy!
%       The top/left/right margins specified in lines 481--488 are
%       exactly what the Dissertation Office specifies, but your printer
%       may print documents offset or stretched by a bit.  One tip is to
%       increase the margins by a fudge factor of 0.05-0.1in; another is
%       to redefine TeX's default \hoffset and \voffset to something
%       other than 1in (e.g., \hoffset=0.95in).  If you do either of 
%       these, PLEASE DON'T PROPAGATE YOUR CHANGES IN THIS FILE.  This
%       class should remain as general-purpose as possible.
%
%       A more elegant solution for offset pages is to use the results
%       of testpage.tex to specify the printer offsets, either in the
%       config.ps in your TeX distribution, or in ~/.dvipsrc.  See the
%       dvips man page for details.
%
% Changes:
%   1) Moved Astronomy-specific modifications to their own package.
%   2) Fixed the bug wherein multipage lists of tables and figures were
%      listed in the TOC with the last page instead of the first.
%   3) Added space between multiple footnotes on a page, as per
%      requirements.  Also made footnote continuation separator the
%      width of text.
%   4) Merged some changes from Martin Pergler's modifications of
%      easychithesis.cls, namely: respace title page a bit, make
%      footnote numbers full size, resize math subscripts, add leader
%      dots to TOC chapters.
%   5) Tweaked margins just slightly to give footer more room. 
%   6) Added "tocchapterhead" option.  This will insert the words 
%      "CHAPTER" and "APPENDIX" as headings in the TOC, although the
%      dissertation format doesn't *require* it.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%       John's modifications to Luisa's myeasychithesis2.cls and new hints
%          for formatting the document file.(last updated 5-JUL-2001, JEV)
%               1) The acknowledgment environment (as in aastex) has
%                  been changed to invoke \topmatter.
%               2) (moved to astroextras)
%               3) If you want '\\' to force a new line in title then
%                  comment out line 356 and uncomment line 355. Note
%                  however, that this then requires entering the title
%                  in all caps in the document file.
%               4) (moved to astroextras)
%               5) Added \makecopyright command from Steve Martin's
%                  thesis macros (lines 283--295).
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%  This is a modification of the 
%
%     easychithesis.cls   Version 1.5   April 5, 1999
%  
%  I (Brad Holden, bph herein) used this for my thesis.  I encountered a
%  few problems when dealing with the Office of Academic
%  Publications so I fixed those in this version.  In the future, 
%  new policies will be enacted so this version will no doubt become
%  obselete as well.
%
%     myeasychithesis.cls   March 13, 2000
%
%
% I added a whole bunch of things from aastex.cls file to 
% enable things like deluxetable to work.  --L. Rebull
%
%      myeasychithesis2.cls   July, 2000   
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%     Original Instructions And Hints
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% This file provides the LaTeX2e style myeasychithesis for formating
% graduate thesis at the University of Chicago.  It was written by
% Bryan Clair and Nathan Dunfield.  For an example of its use see the
% file: template.tex, and the instructions below.
%
% Comments or problems should be sent to holden@oddjob.uchicago.edu
%
%
% Invoking this style:  Start your document with (this is 1.5 spaced)
%
%   \documentclass{myeasychithesis2}
%
% You should submit a double-spaced thesis.  So, start with
%
%   \documentclass[truedoublespace]{myeasychithesis2}
%
% When printing drafts, you may not want your thesis doublespaced.
% If you use instead:
%
%   \documentclass[singlespace]{myeasychithesis2}
%
% or
%
%   \documentclass[onehalfspace]{myeasychithesis2}
%
% your thesis will come out single or 3/2 spaced respectively.
% myeasychithesis2 also understands all options understood by report.
% Also the ``double spacing'' provided by this style is not ``true''
% doublespacing as defined by setspace.sty.  Instead, it is the same
% as on the old LaTeX 2.09 thesis style ``chithesis''.  If you want
% ``true'' double spacing, give the option truedoublespace.
%
% Title Page Information Commands
%    Example Usage:
%       \title{Stones And L^2 Invariants}
%       \author{Fred D. Belitnikoff}
%       \department{Mathematics}
%       \division{Physical Sciences}  (Sciences with an s!)
%       \degree{Doctor of Philosophy}
%       \date{July 1998}    (Your grad. date, formatted like the example)
%       \maketitle
%
% \dedication : Use for a dedication, copyright, or epigraph.
%               Produces a page with no number for the text which follows
%               If you want centering, do it yourself with
%               \begin{center} and \end{center}.
%        Usage:
%           \dedication
%           To blah.
%
% \topmatter : Things like Abstract, Acknowledgments.
%    Usage:
%       \topmatter{Title Of Matter}
%
%       Use like \chapter, once for each page of matter you want.  Don't
%       use numbered sections within these.  Use \section* instead.
%
% The abstract environment has been changed to invoke \topmatter.
% The acknowledgment environment has been changed to invoke
% \topmatter, use it like the aastex acknowledgment command but put
% it at beginning of document for a Chicago Thesis
%
% \mainmatter : Signals the start of the document's body.
%    Usage:
%       \mainmatter
%
%       Use once before the first numbered \chapter.
%
%  HINTS:
%
%  1.  To get appendices, you don't do anything different from a normal
%     report document.  That means, put the command \appendix before
%     you begin your first appendix, then do each appendix with a
%     \chapter command.  Note that if you have only one appendix, it is
%     customary to leave it unnumbered.  Do this with \chapter*.
%
%  2. If you are having page break problems (which are much more
%     likely in double spacing), use the \pagebreak[n] and
%     \nopagebreak[n] commands to tell LaTeX good and bad 
%     places for page breaks. n=1,2,3 for gradually more
%     emphatic suggestion to LaTeX. n=4 is almost always
%     break page or almost never break page.
%
%  3. If you are in a rush and don't have time to go through and
%     rework places which generate overfull hboxes (stuff sticking
%     out from right margin), put the command \sloppy before your
%     \begin{document}. LaTeX will not generate overfull hboxes but will
%     stretch spaces more, beyond its usual aesthetic preferences.
%
%  KNOWN PROBLEMS
%
%  1.  No support for multi-volume theses.
%
%  2.  Doesn't work with older versions of setspace.sty
%
%  3.  Problems with math formulas in chapter headings:
%
%         a.  Any lowercase letters in the formula are converted to
%         uppercase, e.g. f(x) becomes F(X).   If you really need
%         lowercase math letters in your chapter titles, use the
%         option plainchapterheads (and, if you want, type your
%         chapter titles in ALL CAPS so that the appearance doesn't
%         change).  Note there is no problem
%         for section or subsection headings in either case.
%
%         b.  Some perfectly reasonable math commands when used in
%         \chapter give the error
%          ``LATEX ERROR: \command  ALLOWED ONLY IN MATH MODE.''
%         The solution to this is to do
%
%              \newcommand{\mymath}{problem math goes here}
%
%         and then
%
%              \chapter{All about \protect\mymath}
%
%         also, the option plainchapterheads will fix this too.
%
%  Improvements over older versions:
%
%  Added in 1.4: Name of the bibiography is now References, to please the
%  thesis office.  If you want to change it back, put
%
%   \renewcommand{\bibname}{Bibliography}
%
%  in your TeX file prior to \begin{document}
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\NeedsTeXFormat{LaTeX2e}[1995/06/01]
\ProvidesClass{ccw_chithesis}[2006/01/26 Chicago Thesis Class for
LaTex2e]

\RequirePackage{setspace}[1998/04/8]

\newcommand{\etptsize}{12pt}
\DeclareOption{10pt}{\renewcommand{\etptsize}{10pt}}
\DeclareOption{11pt}{\renewcommand{\etptsize}{11pt}}
\DeclareOption{12pt}{\renewcommand{\etptsize}{12pt}}
\DeclareOption{letterpaper}{\PassOptionsToClass{letterpaper}{report}}

% The following options set the global amount of spacing

\newcommand{\etspacing}{\setstretch{1.37}}
\DeclareOption{truedoublespace}{\renewcommand{\etspacing}{\doublespacing}}
\DeclareOption{doublespace}{\renewcommand{\etspacing}{\setstretch{1.37}}}
\DeclareOption{onehalfspace}{\renewcommand{\etspacing}{\onehalfspacing}}
\DeclareOption{singlespace}{\renewcommand{\etspacing}{\singlespacing}}

% Normally, the chapter heads are all uppercase, but sometimes
% this causes problems.  The following option changes
% the chapter heads to small caps instead.

\newcommand{\etchapterheadstyle}[1]{\MakeUppercase{#1}}
\DeclareOption{plainchapterheads}{\renewcommand{\etchapterheadstyle}[1]{#1}}

% This option puts "CHAPTER" and "APPENDIX" headings into the TOC.
% LaTeX omits them by default.  (CCW)

\let\iftocflag=\iffalse
\DeclareOption{tocchapterhead}{\let\iftocflag=\iftrue}

% Pass all other options to report.cls.
\DeclareOption*{%
 \PassOptionsToClass{\CurrentOption}{report}
}

% It would be better to have the following line next,
% rather than setting the defaults manually, but
% this would break early versions of LaTeX2e
%
%\ExecuteOptions{12pt,letterpaper,doublespace}
\ProcessOptions\relax
\LoadClass[\etptsize]{report}

% CCW: Footnote modifications
% Lengthen continuation rule line for footnotes 
\RequirePackage[splitrule]{footmisc}%
% Add spacing between multiple footnotes, but make sure space isn't
% added between rule and notes.  This is a bit of a kludge.
\setlength{\footnotesep}{\baselineskip}
\let\oldfootnoterule = \footnoterule
\renewcommand{\footnoterule}{\kern 0.4\footnotesep\oldfootnoterule\kern-0.4\footnotesep}
% Make footnote numbers in footnotes regular size, as per ``modern practice''
\renewcommand{\@makefntext}[1]{\noindent\makebox[2.5em][r]{\@thefnmark.\ }#1}

% CCW: font size modifications, from Martin Pergler
% Put it at document start, else some packages may clobber it.
% Dissertation Office says minimum should be 9pt for microfilming, but
% they didn't actually complain about the standard LaTeX sizes (12/8/6)
% that I used previously.  So 8pt isn't too much of a cheat.
\AtBeginDocument{\DeclareMathSizes{12}{12}{10}{8}}

% Adjust spacing of document

\etspacing

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%     Commands Defined For Use By Thesis Author
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% \dedication : Use for a dedication, copyright, or epigraph.
%               Produces a page with no number for the text which follows
%               If you want centering, do it yourself with
%               \begin{center} and \end{center}.
%
\newcommand{\dedication}{
        \chapter*{}
        \thispagestyle{empty}
}

% copyright page (if used, should appear directly after title page)
% usage: \makecopyright
\def\makecopyright{\@restonecolfalse
   \if@twocolumn\@restonecoltrue\onecolumn
   \else \newpage \fi
   \thispagestyle{empty}%\setcounter{page}{2}%
%   \addtocounter{page}{1}
   \null\vfill
\centerline{   Copyright \copyright\ \number\year\ by \@author }
\centerline{ All rights reserved }
   \vskip 15pt\relax
   \if@restonecol\twocolumn \else \newpage \fi
   \let\makecopyright=\relax}

%
% \topmatter : Things like Abstract, Acknowledgments.
%    Usage:
%       \topmatter{Title Of Matter}
%
%       Use like \chapter, once for each page of matter you want.  Don't
%       use numbered sections within these.  Use \section* instead.
%
\newcommand{\topmatter}[1]{
        \chapter*{#1}
        \addcontentsline{toc}{chapter}{#1}
}

%
% redefinition of abstract environment
%

\renewenvironment{abstract}{\topmatter{Abstract}}{}
\newenvironment{acknowledgments}{\topmatter{Acknowledgments}}{}

%
% \mainmatter : Signals the start of the document's body.
%    Usage:
%       \mainmatter
%
%       Use once before the first numbered \chapter.
%
\newcommand{\mainmatter}{
        \clearpage
        \pagenumbering{arabic}
}

%
% Title Page Information Commands
%    Example Usage:
%       \title{Stones And L^2 Invariants}
%       \author{Fred D. Belitnikoff}
%       \department{Mathematics}
%       \division{Physical Sciences}  (Sciences with an s!)
%       \date{July 1998}    (Your grad. date, formatted like the example)
%
%
\renewcommand{\title}[1]{\gdef\@title{#1}}
\renewcommand{\author}[1]{\gdef\@author{#1}}
\newcommand{\department}[1]{\gdef\@department{#1}}
\newcommand{\division}[1]{\gdef\@division{#1}}
\newcommand{\degree}[1]{\gdef\@degree{#1}}
\renewcommand{\date}[1]{\gdef\@date{#1}}

%
% \maketitle :  Makes the U. of Chicago dissertation title page.
%
\renewcommand{\maketitle}{
  \begin{titlepage}
    \begin{center}
      \rule{0in}{0.25in}
      THE UNIVERSITY OF CHICAGO\\
      \vfill
%      \@title\\
      \MakeUppercase{\@title}\\
      \vfill
      A PAPER SUBMITTED TO\\
      THE FACULTY OF THE DIVISION OF THE \MakeUppercase{\@division}\\
      IN CANDIDACY FOR THE DEGREE OF\\
      \MakeUppercase{\@degree}\\
      \ \\
      DEPARTMENT OF \MakeUppercase{\@department}\\
      \vfill
      BY\\
      \MakeUppercase{\@author}\\
      \vfill
      CHICAGO, ILLINOIS\\
      \MakeUppercase{\@date}
      \vspace*{0.45in}
    \end{center}
  \end{titlepage}
  \pagenumbering{roman}
  \addtocounter{page}{1}
}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%     Internal Formatting Commands For This Thesis Style
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Redo commands that make chapter & section headers
%
\renewcommand{\@makechapterhead}[1]{{

        \setlength{\topmargin}{0.5in}   % really 1.5in, added by bph
                                        % to meet OAP standards
        \normalfont\large\bfseries
        \begin{center}
        \etchapterheadstyle{\@chapapp{ \thechapter}}  %  CHAPTER 1 or APPENDIX


        \etchapterheadstyle{#1}                       %     TITLE OF CHAPTER
        \end{center}
        \setlength{\topmargin}{0in}   % back to 1in

}}
\renewcommand{\@makeschapterhead}[1]{{  
        \setlength{\topmargin}{0.5in}   % really 1.5in, added by bph
                                        % to meet OAP standards
        \normalfont\large\bfseries
        \begin{center}
        \etchapterheadstyle{#1}                       %     TITLE OF CHAPTER
        \end{center}
        \setlength{\topmargin}{0in}   % back to 1in
}}
\renewcommand\section{\@startsection {section}{1}{\z@}%
                                {-3.5ex \@plus -1ex \@minus -.2ex}%
                                {2.3ex \@plus.2ex}%
                                {\normalfont\large\bfseries\centering}}
\renewcommand\subsection{\@startsection{subsection}{2}{\z@}%
                                {-3.25ex\@plus -1ex \@minus -.2ex}%
                                {1.5ex \@plus .2ex}%
                                {\normalfont\large\itshape\centering}}
\renewcommand\subsubsection{\@startsection{subsubsection}{3}{\z@}%
                                {-3.25ex\@plus -1ex \@minus -.2ex}%
                                {1.5ex \@plus .2ex}%
                                {\normalfont\large}}
\renewcommand\paragraph{\@startsection{paragraph}{4}{\z@}%
                                {3.25ex \@plus1ex \@minus.2ex}%
                                {-1em}%
                                {\normalfont\normalsize\bfseries}}
\renewcommand\subparagraph{\@startsection{subparagraph}{5}{\parindent}%
                                {3.25ex \@plus1ex \@minus .2ex}%
                                {-1em}%
                                {\normalfont\normalsize\itshape}}

% Change style of printing chapters in TOC to match chapter headings.

\renewcommand*\l@chapter[2]{%
  \ifnum \c@tocdepth >\m@ne
    \addpenalty{-\@highpenalty}%
    \vskip 1.0em \@plus\p@
    \setlength\@tempdima{1.5em}%
    \begingroup
      \parindent \z@ \rightskip \@pnumwidth
      \parfillskip -\@pnumwidth
      \leavevmode
      \advance\leftskip\@tempdima
      \hskip -\leftskip
      \etchapterheadstyle{#1}\nobreak
% CCW: The following 3 lines add dots to the chapter TOC listings
      \leaders\hbox{$\m@th
        \mkern \@dotsep mu\hbox{.}\mkern \@dotsep
        mu$}\hfill
      \hfil \nobreak\hb@xt@\@pnumwidth{\hss #2}\par
      \penalty\@highpenalty
    \endgroup
  \fi}

% Add "CHAPTER"/"APPENDIX" headings to TOC before first of each.
% If you want these words to be a bit bigger, remove "\normalsize" 
% from the definition below.  If you want the words to be bold, 
% remove "\mdseries". (CCW)

\iftocflag
\let\old@chapter = \@chapter
\def\@chapter[#1]#2{\ifnum\c@chapter=\z@%
  \addtocontents{toc}{\protect\contentsline{part}%
    {\mdseries\normalsize\etchapterheadstyle{\@chapapp}}{}}%
  \fi%
  \old@chapter[{#1}]{#2}}
\fi
 
% Change names of table of contents and bibliography

\renewcommand{\contentsname}{Table of Contents}
\renewcommand{\bibname}{References}

% Change the margins to fit requirements

\setlength{\oddsidemargin}{0.5in}  % really 1.5in
\setlength{\evensidemargin}{0.5in}  % really 1.5in
\setlength{\textwidth}{6in}
\setlength{\topmargin}{0in}   % really 1in
\setlength{\headheight}{0.2in}
\setlength{\headsep}{0.2in}
\setlength{\textheight}{8.5in}
\setlength{\footskip}{0.3in}

% Changing where the page numbers go on pages that aren't the first
% page of a chapter.

\pagestyle{myheadings}
\markright{}

% Make the table of contents, etc.,  single spaced
%
% bph added lists of tables/figures to table of contents.
% CCW fixed bug where multipage LOT/LOF were listed in TOC with wrong
% page number.

\let\oldtableofcontents = \tableofcontents
\renewcommand{\tableofcontents}{
  \begin{singlespace}
    \oldtableofcontents
  \end{singlespace}
}

\let\oldlistoftables = \listoftables
\renewcommand{\listoftables}{
  \begin{singlespace}
    % This works b/c multiple \newpage's are treated as one
    \newpage
    \addcontentsline{toc}{chapter}{List of Tables}
    \oldlistoftables 
  \end{singlespace}
}

\let\oldlistoffigures = \listoffigures
\renewcommand{\listoffigures}{
  \begin{singlespace}
    % This works b/c multiple \newpage's are treated as one
    \newpage
    \addcontentsline{toc}{chapter}{List of Figures}
    \oldlistoffigures 
  \end{singlespace}
}

% End of File
