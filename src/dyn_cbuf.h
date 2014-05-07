/****************************************************************************
*
*   Since this code originated from code which is public domain, I
*   hereby declare this code to be public domain as well.
*
*   Dave Hylands - dhylands@gmail.com
*
****************************************************************************/
/**
*
*   @file   CBUF.h
*
*   @defgroup   CBUF Circular Buffer
*   @{
*
*   @brief  A simple and efficient set of circular buffer manipulations.
*
*   These macros implement a circular buffer which employs get and put
*   pointers, in such a way that mutual exclusion is not required
*   (assumes one reader & one writer).
*
*   It requires that the circular buffer size be a power of two, and the
*   size of the buffer needs to smaller than the index. So an 8 bit index
*   supports a circular buffer upto ( 1 << 7 ) = 128 entries, and a 16 bit index
*   supports a circular buffer upto ( 1 << 15 ) = 32768 entries.
*
*   The basis for these routines came from an article in Jack Ganssle's
*   Embedded Muse: http://www.ganssle.com/tem/tem110.pdf
*
*   In order to offer the most amount of flexibility for embedded environments
*   you need to define a macro for the size.
*
*   First, you need to name your circular buffer. For this example, we'll
*   call it @c myQ.
*
*   The size macro that needs to be defined will be the name of the
*   circular buffer followed by @c _SIZE. The size must be a power of two
*   and it needs to fit in the get/put indicies. i.e. if you use an
*   8 bit index, then the maximum supported size would be 128.
*
*   The structure which defines the circular buffer needs to have 3 members
*   <tt>m_getIdx, m_putIdx,</tt> and @c m_entry.
*
*   @c m_getIdx and @c m_putIdx need to be unsigned integers of the same size.
*
*   @c m_entry needs to be an array of @c xxx_SIZE entries, or a pointer to an
*   array of @c xxx_SIZE entries. The type of each entry is entirely up to the
*   caller.
*
*   @code
*   #define myQ_SIZE    64
*   
*   volatile struct
*   {
*       uint8_t     m_getIdx;
*       uint8_t     m_putIdx;
*       uint8_t     m_entry[ myQ_SIZE ];
*
*   } myQ;
*   @endcode
*
*   You could then use CBUF_Push to add a character to the circular buffer:
*
*   @code
*   CBUF_Push( myQ, 'x' );
*   @endcode
*
*   And CBUF_Pop to retrieve an element from the buffer:
*
*   @code
*   ch = CBUF_Pop( myQ );
*   @endcode
*
*
****************************************************************************/

#if !defined( DYN_CBUF_H )
#define DYN_CBUF_H      



/** 
*   Initializes the circular buffer for use.
*/ 

#define CBUF_Init( cbuf )       cbuf.m_getIdx = cbuf.m_putIdx = 0

/**
*   Returns the number of elements which are currently
*   contained in the circular buffer.
*/

#define CBUF_Len( cbuf )        ((typeof( cbuf.m_putIdx ))(( cbuf.m_putIdx ) - ( cbuf.m_getIdx )))

/**
*   Appends an element to the end of the circular buffer. The
*   element is expected to be of the same type as the @c m_entry
*   member.
*/

#define CBUF_Push( cbuf, elem ) (cbuf.m_entry)[ cbuf.m_putIdx++ & (( cbuf##_SIZE ) - 1 )] = (elem)

/**
*   Retrieves an element from the beginning of the circular buffer
*/

#define CBUF_Pop( cbuf )        (cbuf.m_entry)[ cbuf.m_getIdx++ & (( cbuf##_SIZE ) - 1 )]

/**
*   Returns a pointer to the last spot that was pushed.
*/

#define CBUF_GetLastEntryPtr( cbuf ) &(cbuf.m_entry)[ ( cbuf.m_putIdx - 1 ) & (( cbuf##_SIZE ) - 1 )]

/**
*   Returns a pointer to the next spot to push. This can be used
*   in conjunction with CBUF_AdvancePushIdx to fill out an entry
*   before indicating that it's available. It is the caller's
*   responsibility to enure that space is available, and that no
*   other items are pushed to overwrite the entry returned.
*/

#define CBUF_GetPushEntryPtr( cbuf ) &(cbuf.m_entry)[ cbuf.m_putIdx & (( cbuf##_SIZE ) - 1 )]

/**
*   Advances the put index. This is useful if you need to
*   reserve space for an item but can't fill in the contents
*   yet. CBUG_GetLastEntryPtr can be used to get a pointer to
*   the item. It is the caller's responsibility to ensure that
*   the item isn't popped before the contents are filled in.
*/

#define CBUF_AdvancePushIdx( cbuf )  cbuf.m_putIdx++

/**
*   Advances the get index. This is slightly more efficient than
*   popping and tossing the result.
*/

#define CBUF_AdvancePopIdx( cbuf )  cbuf.m_getIdx++

/**
*   Retrieves the <tt>idx</tt>'th element from the beginning of
*   the circular buffer
*/

#define CBUF_Get( cbuf, idx )        (cbuf.m_entry)[( cbuf.m_getIdx + idx ) & (( cbuf##_SIZE ) - 1 )]

/**
*   Retrieves the <tt>idx</tt>'th element from the end of the
*   circular buffer.
*/

#define CBUF_GetEnd( cbuf, idx )        (cbuf.m_entry)[( cbuf.m_putIdx - idx - 1 ) & (( cbuf##_SIZE ) - 1 )]

/**
*   Returns a pointer to the next spot to push.
*/

#define CBUF_GetPopEntryPtr( cbuf ) &(cbuf.m_entry)[ cbuf.m_getIdx & (( cbuf##_SIZE ) - 1 )]

/**
*   Determines if the circular buffer is empty.
*/

#define CBUF_IsEmpty( cbuf )    ( CBUF_Len( cbuf ) == 0 )

/**
*   Determines if the circular buffer is full.
*/

#define CBUF_IsFull( cbuf )     ( CBUF_Len( cbuf ) == ( cbuf##_SIZE ))

/**
*   Determines if the circular buffer is currenly overflowed or underflowed.
*/

#define CBUF_Error( cbuf )      ( CBUF_Len( cbuf ) > cbuf##_SIZE )



#endif // DYN_CBUF_H
