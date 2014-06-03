/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */


#ifndef _DYN_RING_QUEUE_
#define _DYN_RING_QUEUE_



#define C2G_InQ_SIZE     256
#define C2G_OutQ_SIZE    256

volatile struct
{
     long     m_getIdx;
     long     m_putIdx;
     void*    m_entry[C2G_InQ_SIZE];
} C2G_InQ;



volatile struct
{
     long     m_getIdx;
     long     m_putIdx;
     void*    m_entry[C2G_OutQ_SIZE];
} C2G_OutQ;



#endif
