/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:     Blob callbacks for the native VOL connector
 */

/***********/
/* Headers */
/***********/
#include "H5private.h"          /* Generic Functions                    */
#include "H5Eprivate.h"         /* Error handling                       */
#include "H5Fprivate.h"         /* File access				*/
#include "H5HGprivate.h"	/* Global Heaps				*/
#include "H5VLnative_private.h" /* Native VOL connector                 */


/****************/
/* Local Macros */
/****************/


/******************/
/* Local Typedefs */
/******************/


/********************/
/* Local Prototypes */
/********************/


/*********************/
/* Package Variables */
/*********************/


/*****************************/
/* Library Private Variables */
/*****************************/


/*******************/
/* Local Variables */
/*******************/



/*-------------------------------------------------------------------------
 * Function:    H5VL__native_blob_put
 *
 * Purpose:     Handles the blob 'put' callback
 *
 * Return:      SUCCEED / FAIL
 *
 * Programmer:	Quincey Koziol
 *		Friday, August 15, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_blob_put(void *blob, size_t size, void *_ctx, void *_id)
{
    uint8_t *vl = (uint8_t *)_id;       /* Pointer to the user's hvl_t information */
    H5VL_blob_put_ctx_t *ctx = (H5VL_blob_put_ctx_t *)_ctx;     /* Context info from caller */
    H5HG_t hobjid;                      /* New VL sequence's heap ID */
    herr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check parameters */
    HDassert(vl);
    HDassert(size == 0 || blob);
    HDassert(ctx);
    HDassert(ctx->f);

    /* Write the VL information to disk (allocates space also) */
    if(H5HG_insert(ctx->f, size, blob, &hobjid) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_WRITEERROR, FAIL, "unable to write VL information")

    /* Set the length of the sequence */
    UINT32ENCODE(vl, ctx->seq_len);

    /* Encode the heap information */
    H5F_addr_encode(ctx->f, &vl, hobjid.addr);
    UINT32ENCODE(vl, hobjid.idx);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_blob_put() */


/*-------------------------------------------------------------------------
 * Function:    H5VL__native_blob_get
 *
 * Purpose:     Handles the blob 'get' callback
 *
 * Return:      SUCCEED / FAIL
 *
 * Programmer:	Quincey Koziol
 *		Friday, August 15, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_blob_get(const void *_id, void *_ctx, void *buf)
{
    const uint8_t *vl = (const uint8_t *)_id; /* Pointer to the disk blob information */
    H5F_t *f = (H5F_t *)_ctx;           /* Retrieve file pointer from context */
    H5HG_t hobjid;                      /* Global heap ID for sequence */
    herr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    HDassert(vl);
    HDassert(f);
    HDassert(buf);

    /* Skip the length of the sequence */
    vl += 4;

    /* Get the heap information */
    H5F_addr_decode(f, &vl, &hobjid.addr);
    UINT32DECODE(vl, hobjid.idx);

    /* Check if this sequence actually has any data */
    if(hobjid.addr > 0)
        /* Read the VL information from disk */
        if(NULL == H5HG_read(f, &hobjid, buf, NULL))
            HGOTO_ERROR(H5E_DATATYPE, H5E_READERROR, FAIL, "unable to read VL information")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_blob_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL__native_blob_specific
 *
 * Purpose:     Handles the blob 'specific' callback
 *
 * Return:      SUCCEED / FAIL
 *
 * Programmer:	Quincey Koziol
 *		Friday, August 15, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_blob_specific(void *id, H5VL_blob_specific_t specific_type,
    va_list arguments)
{
    herr_t ret_value = SUCCEED;     /* Return value */

    FUNC_ENTER_PACKAGE

    switch(specific_type) {
        case H5VL_BLOB_GETSIZE:
            {
                const uint8_t *vl = (const uint8_t *)id; /* Pointer to the disk VL information */
                size_t *size = HDva_arg(arguments, size_t *);

                /* Get length of sequence */
                UINT32DECODE(vl, *size);

                break;
            }

        case H5VL_BLOB_ISNULL:
            {
                const uint8_t *vl = (const uint8_t *)id; /* Pointer to the disk VL information */
                H5F_t *f = HDva_arg(arguments, H5F_t *);
                hbool_t *isnull = HDva_arg(arguments, hbool_t *);
                haddr_t addr;               /* Sequence's heap address */

                /* Skip the sequence's length */
                vl += 4;

                /* Get the heap address */
                H5F_addr_decode(f, &vl, &addr);

                /* Check if heap address is 'nil' */
                *isnull = (addr == 0 ? TRUE : FALSE);

                break;
            }

        case H5VL_BLOB_SETNULL:
            {
                uint8_t *vl = (uint8_t *)id; /* Pointer to the disk VL information */
                H5F_t *f = HDva_arg(arguments, H5F_t *);

                /* Set the length of the sequence */
                UINT32ENCODE(vl, 0);

                /* Encode the "nil" heap pointer information */
                H5F_addr_encode(f, &vl, (haddr_t)0);
                UINT32ENCODE(vl, 0);

                break;
            }

        case H5VL_BLOB_DELETE:
            {
                const uint8_t *vl = (const uint8_t *)id; /* Pointer to the disk VL information */
                H5F_t *f = HDva_arg(arguments, H5F_t *);
                size_t seq_len;             /* VL sequence's length */

                /* Get length of sequence */
                UINT32DECODE(vl, seq_len);

                /* Delete object, if length > 0 */
                if(seq_len > 0) {
                    H5HG_t hobjid;              /* VL sequence's heap ID */

                    /* Get heap information */
                    H5F_addr_decode(f, &vl, &(hobjid.addr));
                    UINT32DECODE(vl, hobjid.idx);

                    /* Free heap object */
                    if(hobjid.addr > 0)
                        if(H5HG_remove(f, &hobjid) < 0)
                            HGOTO_ERROR(H5E_VOL, H5E_CANTREMOVE, FAIL, "unable to remove heap object")
                } /* end if */

                break;
            }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid specific operation")
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_blob_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL__native_blob_optional
 *
 * Purpose:     Handles the blob 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 * Programmer:	Quincey Koziol
 *		Friday, August 15, 2019
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_blob_optional(void *id, va_list arguments)
{
    herr_t ret_value = SUCCEED;     /* Return value */

    FUNC_ENTER_PACKAGE

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_blob_optional() */

