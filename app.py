import streamlit as st
import pandas as pd
from reconcile import reconcile_transactions

st.set_page_config(page_title="Reconciliation System", layout="wide")

st.title("💳 Transaction Reconciliation System")

# Upload files
st.sidebar.header("Upload CSV Files")

system_file = st.sidebar.file_uploader("Upload System Transactions CSV", type=["csv"])
bank_file = st.sidebar.file_uploader("Upload Bank Transactions CSV", type=["csv"])

if system_file and bank_file:
    system_df = pd.read_csv(system_file)
    bank_df = pd.read_csv(bank_file)

    st.success("Files uploaded successfully ✅")

    # Show data preview
    with st.expander("📊 Preview Data"):
        st.subheader("System Transactions")
        st.dataframe(system_df)

        st.subheader("Bank Transactions")
        st.dataframe(bank_df)

    # Run reconciliation
    if st.button("🔍 Run Reconciliation"):
        results = reconcile_transactions(system_df, bank_df)

        st.header("📈 Results Summary")

        col1, col2, col3 = st.columns(3)

        col1.metric("Matched", len(results["matched"]))
        col1.metric("Missing in Bank", len(results["missing_in_bank"]))

        col2.metric("Missing in System", len(results["missing_in_system"]))
        col2.metric("Duplicates", len(results["duplicates"]))

        col3.metric("Amount Mismatch", len(results["amount_mismatch"]))
        col3.metric("Refund Issues", len(results["refund_no_original"]))

        # Show mismatches
        st.subheader("⚠️ Mismatches")

        all_mismatches = []
        for category, records in results.items():
            if category != "matched":
                for r in records:
                    r["category"] = category
                    all_mismatches.append(r)

        if all_mismatches:
            mismatch_df = pd.DataFrame(all_mismatches)
            st.dataframe(mismatch_df)

            # Download button
            csv = mismatch_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "⬇️ Download Mismatches CSV",
                csv,
                "mismatches.csv",
                "text/csv"
            )
        else:
            st.success("No mismatches found 🎉")

else:
    st.info("👈 Upload both CSV files to start")
